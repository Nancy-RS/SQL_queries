WITH last_payments AS (SELECT rental_month_id,
                              user_id,
                              MAX(paid_at) AS last_payment_at
                         FROM mongo_rental_payments 
                        GROUP BY rental_month_id, user_id
),
home_locations AS (SELECT mongo_homes_id,
                          MIN(value) AS latitude,
                          MAX(value) AS longitude
                     FROM mongo_homes_location
                    GROUP BY mongo_homes_id
),  
interest_historically_paid AS (SELECT rental_month_id,
                                      SUM(CASE
                                          WHEN interests IS NULL THEN 0::numeric
                                          ELSE ABS(interests)
                                          END
                                         ) AS interest_paid
                                 FROM mongo_interest_historics
                                WHERE interests <= 0::numeric 
                                   OR interests IS NULL
                                GROUP BY rental_month_id
),
interests_data AS (SELECT interest_rents.rental_month_id, 
                          interest_rents.interest_pending,
                          historical_interest_payments.interest_paid
                     FROM (SELECT historical_interests.rental_month_id,
                                  historical_interests.created_at,
                                  historical_interests.interests AS interest_pending,
                                  rental_months._status,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY historical_interests.rental_month_id
                                      ORDER BY historical_interests.rental_month_id, historical_interests.created_at DESC
                                  ) AS ranking
                             FROM mongo_interest_historics AS historical_interests
                                  LEFT JOIN mongo_rental_months AS rental_months 
                                  ON rental_months.id = historical_interests.rental_month_id
                            WHERE rental_months._status IN ('pending', 'partial_paid')
                            ORDER BY historical_interests.rental_month_id, historical_interests.created_at DESC
                          ) AS interest_rents
                          LEFT JOIN interest_historically_paid AS historical_interest_payments 
                          ON interest_rents.rental_month_id = historical_interest_payments.rental_month_id
                    WHERE interest_rents.ranking = 1
),
deposit_by_rent AS (SELECT rent_id,
                           SUM(deposit_amount) deposito
                      FROM mongo_rental_months
                     GROUP BY rent_id
),
historic_deposit_by_rent AS (SELECT deposit_by_rent.rent_id,
                                    ROUND(COALESCE(deposit_by_rent.deposito, 0) + 
                                          COALESCE(deposit_by_rent_rents.deposito, 0) + 
                                          COALESCE(deposit_by_rent_parents.deposito, 0)
                                    ) AS deposito
                               FROM deposit_by_rent
                                    LEFT JOIN mongo_rents AS rents 
                                    ON rents._id = deposit_by_rent.rent_id

                                    LEFT JOIN deposit_by_rent AS deposit_by_rent_rents
                                    ON rents.parent_rent_id = deposit_by_rent_rents.rent_id

                                    LEFT JOIN mongo_rents AS parent_rents 
                                    ON rents.parent_rent_id = parent_rents._id

                                    LEFT JOIN deposit_by_rent AS deposit_by_rent_parents
                                    ON parent_rents.parent_rent_id = deposit_by_rent_parents.rent_id
)						
SELECT rents._id AS "id de renta",
       rents.user_id AS "id de usuario",
       rental_months._id AS "id mes de renta",
       --rents.home_application_id AS "id de aplicacion del inmueble",
       --owners.user_id AS "Id del Propietario",
       rents.home_id AS "id de inmueble",
       CASE
       WHEN ((home_features.extras & (1 << 5)) > 0) IS NULL THEN homes.furnished
       ELSE ((home_features.extras & (1 << 5)) > 0)
       END AS amueblado,
       rental_months._charge_type AS "tipo de cargo",
       INITCAP(users.name) AS "nombre del inquilino",
       INITCAP(users.last_name||' '||users.second_last_name) AS "apellido del inquilino",
       users.email AS "email del inquilino",
       INITCAP(users_owners.name) AS "nombre del propietario",
       INITCAP(users_owners.last_name||' '|| users_owners.second_last_name) AS "apellido del propietario",
       users_owners.email AS "email del propietario",
       --rental_months._id AS "Id tabla Meses de Renta",
       --LPAD(date_part('month', to_date(rental_months.month, 'Month'::TEXT))::TEXT, 2, '0')|| '_'||rental_months.year AS "Mes y ano",
       TO_DATE('1'||'-'|| rental_months.month||' '||rental_months.year, 'DD-Month-YYYY')::DATE AS mes,
       rents.rent_price AS "precio de renta",   
       CONVERT_TIMEZONE('America/Mexico_City', last_payments.last_payment_at) AS "fecha de pago",
       /*CASE WHEN round(rental_months.homie_fee,2) NOT IN (0.05, 0.25, 0.09) THEN 'otros'::TEXT
       ELSE to_char(round(rental_months.homie_fee,2), '0D99')
       END AS "tarifa Homie",
       'yet to come' AS "tipo de configuracion",*/
       --'yet to come' AS "porcentaje de tarifa homie",
       CASE
       WHEN rental_months.homie_fee IS NOT NULL AND payment_method_fee._config_type = 'homie_only_receive_fee'::TEXT THEN 0
       WHEN rental_months.homie_fee IS NULL THEN 0
       ELSE rental_months.homie_fee
       END AS "Porcentaje Comisión",
       CASE WHEN rents.parent_rent_id IS NOT NULL THEN 'Si'
       ELSE 'No'
       END AS "Renovación?",
       rent_contracts.started_at::DATE AS "Fecha de Inicio",
       rent_contracts.ends_at::DATE AS "Fecha de Finalizacion",
       CASE WHEN rent_contracts.signed_at IS NOT NULL THEN 1
       ELSE 0
       END AS "contrato online",
       --'yet to come' AS "notificacion de desalojo",
       --'yet to come' AS "interes generado",
       interests_data.interest_paid AS "interes pagado",
       --'yet to come' AS "interes perdonado",
       --interests_data.interest_pending AS "interes pendiente",
       rents.product AS "producto",
       rental_months.due_date AS "fecha de vencimiento",
       homes.zip AS "codigo postal",
       --'yet to come' AS estado
       historic_deposit_by_rent.deposito AS DeGa,
       CASE WHEN deposit_schemes.name = 'A' AND CONVERT_TIMEZONE('America/Mexico_City', home_applications.created_at) >= '2021-02-01' THEN 0
       ELSE GREATEST(historic_deposit_by_rent.deposito, rents.rent_price)::FLOAT 
       END AS "DeGa Estimado"
  FROM mongo_rents AS rents
       LEFT JOIN mongo_homes AS homes 
       ON (rents.home_id = homes._id)

       LEFT JOIN mongo_users AS users
       ON (rents.user_id = users._id)

       LEFT JOIN mongo_owners AS owners
       ON (homes.owner_id = owners._id)

       LEFT JOIN mongo_home_features AS home_features
       ON (homes._id = home_features.home_id)

       LEFT JOIN mongo_users AS users_owners
       ON (owners.user_id = users_owners._id)

       LEFT JOIN home_locations
       ON (homes.id = home_locations.mongo_homes_id)

       LEFT JOIN mongo_rental_months AS rental_months
       ON (rents._id = rental_months.rent_id)

       LEFT JOIN last_payments
       ON (rental_months._id = last_payments.rental_month_id)

       LEFT JOIN interests_data 
       ON (rental_months._id = interests_data.rental_month_id)

       LEFT JOIN mongo_rental_months_payment_method_fee AS payment_method_fee
       ON payment_method_fee.mongo_rental_months_id = rental_months.id

       LEFT JOIN historic_deposit_by_rent
       ON (rents._id = historic_deposit_by_rent.rent_id)

       LEFT JOIN mongo_rent_contracts AS rent_contracts
       ON rents._id = rent_contracts.rent_id

       LEFT JOIN mongo_home_applications AS home_applications
       ON rents.home_application_id = home_applications._id

       LEFT JOIN mongo_guarantee_deposit_schemes AS deposit_schemes
       ON home_applications.guarantee_deposit_scheme_id = deposit_schemes._id 
 WHERE ((rents._status != ALL (ARRAY['canceled'::TEXT, 
                                     'finished_early'::TEXT, 
                                     'negotiation'::TEXT]))
    OR rental_months._status != 'pending'::TEXT)
   AND rental_months._status != 'canceled'::TEXT AND rental_months._charge_type != 'advance'::TEXT
   AND TO_DATE('1'||'-'|| rental_months.month||' '||rental_months.year, 'DD-Month-YYYY')::DATE >= '2018-01-01'
 ORDER BY rental_months.year,
          rental_months.month,
          rental_months._charge_type,
          CASE
          WHEN ROUND(rental_months.homie_fee,2) NOT IN (0.05, 0.25, 0.09) THEN 'otros'::TEXT
          ELSE TO_CHAR(ROUND(rental_months.homie_fee,2), '0D99')
          END,
          homes.price
