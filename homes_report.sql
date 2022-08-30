WITH home_logs_unique AS (SELECT home_id,
                                 id,
                                 CONVERT_TIMEZONE('America/Mexico_City', created_at) AS created_at,
                                 LAG (status, 1) OVER (
                                     PARTITION BY home_id
                                     ORDER BY created_at
                                 ) AS previous_status,
                                 LAG (created_at, 1) OVER (
                                     PARTITION BY home_id
                                     ORDER BY created_at
                                 ) AS previous_published_at,
                                 status,
                                 changed_by_id,
                                 reason
                            FROM mongo_home_logs
),
published_apartments AS (SELECT home_id,previous_published_at,
                                created_at AS published_at,
                                previous_status,
                                status,
                                changed_by_id
                           FROM home_logs_unique
                          WHERE (previous_status IS NULL
                             OR previous_status != 'published')
                            AND status = 'published'
                          ORDER BY home_id, created_at DESC
),
data_previous_published_at AS (SELECT home_id,
                                      previous_status,
                                      status,
                                      LAG (published_at, 1) OVER (
                                          PARTITION BY home_id
                                          ORDER BY published_at
                                      ) AS previous_published_at,
                                      published_at
                                 FROM published_apartments
),
filter_data_published_for_at_least_2month AS (SELECT home_id,
                                                     previous_status,
                                                     status,
                                                     previous_published_at,
                                                     published_at AS reactivated_at
                                                FROM data_previous_published_at
                                               WHERE DATEDIFF(YEAR, previous_published_at, published_at) * 12 +
                                                     DATEDIFF(MONTH, previous_published_at, published_at) >= 2
),
first_approved_by AS (SELECT home_id,
                             changed_by_id,
                             first_published_at
                        FROM (SELECT home_id,
                                     changed_by_id,
                                     published_at AS first_published_at,
                                     ROW_NUMBER () OVER (
                                         PARTITION BY home_id
                                         ORDER BY home_id, published_at ASC
                                     ) AS ordering_approved_by
                                FROM published_apartments) AS homes
                       WHERE ordering_approved_by = 1
),
last_approved_by AS (SELECT home_id,
                            changed_by_id,
                            last_published_at
                       FROM (SELECT home_id,
                                    changed_by_id,
                                    published_at AS last_published_at,
                                    ROW_NUMBER () OVER (
                                        PARTITION BY home_id
                                        ORDER BY home_id, published_at DESC
                                    ) AS ordering_published
                               FROM published_apartments) AS homes_published
                      WHERE ordering_published = 1
),
bookeed_apartments AS (SELECT home_id,
                              booked_date,
                              amount_booked
                         FROM (SELECT rents.home_id,
                                      CONVERT_TIMEZONE('America/Mexico_City', rental_months.created_at) AS booked_date,
                                      rental_months.amount AS amount_booked,
                                      ROW_NUMBER () OVER (
                                          PARTITION BY home_id
                                          ORDER BY rents.home_id, rental_months.created_at DESC
                                      ) AS ordering_bookeed
                                 FROM mongo_rental_months AS rental_months
                                      LEFT JOIN mongo_rents AS rents
                                      ON rental_months.rent_id = rents._id
                                WHERE rental_months._charge_type = 'advance'
                                  AND rental_months._status = 'paid'
                              ) AS homes
                        WHERE ordering_bookeed = 1
),
first_pending_approval AS (SELECT home_id,
                                  created_at
                             FROM (SELECT home_id,
                                          created_at,
                                          ROW_NUMBER () OVER (
                                              PARTITION BY home_id
                                              ORDER BY created_at ASC
                                          ) AS ordering_logs
                                     FROM home_logs_unique
                                    WHERE previous_status = 'in_progress'
                                      AND status = 'pending_approval'
                                  ) AS first_log
                            WHERE ordering_logs = 1
),
last_pending_approval AS (SELECT home_id,
                                  created_at
                             FROM (SELECT home_id,
                                          created_at,
                                          ROW_NUMBER () OVER (
                                              PARTITION BY home_id
                                              ORDER BY created_at DESC
                                          ) AS ordering_logs
                                     FROM home_logs_unique
                                    WHERE previous_status IN ('in_progress', 'rented')
                                      AND status = 'pending_approval'
                                  ) AS last_log
                            WHERE ordering_logs = 1
),
unpublished_at AS (SELECT home_id,
                          created_at,
                          status,
                          reason
                     FROM (SELECT home_id,
                                  created_at,
                                  status,
                                  reason,
                                  ROW_NUMBER () OVER (
                                      PARTITION BY home_id
                                      ORDER BY home_id, created_at DESC
                                  ) AS ordering_logs
                             FROM home_logs_unique
                            WHERE status != previous_status
                              AND status = ANY (ARRAY ['canceled', 'unpublished','temporarily_canceled'])
                          ) AS homes
                    WHERE ordering_logs = 1
),
previous_price_report AS (SELECT home_id,
                                 previous_price,
                                 current_price,
                                 change_date,
                                 creation_date
                            FROM (SELECT home_logs.home_id,
                                         homes.price AS current_price,
                                         CONVERT_TIMEZONE('America/Mexico_City', home_logs.updated_at) AS change_date,
                                         CONVERT_TIMEZONE('America/Mexico_City', home_logs.created_at) AS creation_date,
                                         LAG (previous_values_price.value, 1) OVER (
                                             PARTITION BY home_logs.home_id
                                             ORDER BY home_logs.home_id
                                         ) AS previous_price,
                                         ROW_NUMBER () OVER (
                                             PARTITION BY home_logs.home_id
                                             ORDER BY home_logs.home_id, home_logs.updated_at DESC
                                         ) AS ordering_previous_price
                                    FROM mongo_homes AS homes
                                         LEFT JOIN mongo_home_logs AS home_logs
                                         ON home_logs.home_id = homes._id

                                         LEFT JOIN mongo_home_logs_previous_values AS previous_values_log
                                         ON previous_values_log.mongo_home_logs_id = home_logs.id

                                         LEFT JOIN mongo_home_logs_previous_values_price AS previous_values_price
                                         ON previous_values_price.mongo_home_logs_previous_values_id = previous_values_log.id
                                   WHERE previous_values_price.value IS NOT NULL
                                     AND CONVERT_TIMEZONE('America/Mexico_City', home_logs.updated_at) >= '2020-09-01'::DATE
                                     AND CONVERT_TIMEZONE('America/Mexico_City', home_logs.updated_at) <= CURRENT_TIMESTAMP
                                   ORDER BY home_logs.home_id,home_logs.updated_at
                                  ) AS homes
                           WHERE ordering_previous_price = 1
),
super_heroe AS (SELECT home_id,
                       _super_hero_type,
                       super_heroe
                  FROM (SELECT homes._id AS home_id,
                               super_heroes._super_hero_type,
                               super_heroes.email AS super_heroe,
                               ROW_NUMBER () OVER (
                                   PARTITION BY homes._id, super_heroes._super_hero_type
                                   ORDER BY homes._id, homes.created_at DESC
                               ) AS ordering_sh
                          FROM mongo_homes AS homes
                               LEFT JOIN mongo_owners AS owners
                               ON homes.owner_id = owners._id

                               LEFT JOIN mongo_users AS users
                               ON owners.user_id = users._id

                               LEFT JOIN mongo_users_super_heroes AS super_heroes
                               ON users.id = super_heroes.mongo_users_id
                         WHERE super_heroes._super_hero_type IN ('customer_success', 'sells')
                         ORDER BY homes._id
                       ) AS homes
                 WHERE ordering_sh = 1
),
rented AS (SELECT *
             FROM (SELECT rents.home_id,
                          rents._status,
                          CONVERT_TIMEZONE('America/Mexico_City', rents.created_at) AS created_at,
                          rents.product,
                          INITCAP(NVL(users.name, '') || ' ' || NVL(users.last_name, '') || ' ' || NVL(users.second_last_name, '')) AS nombre_inquilino,
                          users.email AS email_inquilino,
                          users.mobile_phone AS telefono_inquilino,
                          ROW_NUMBER () OVER (
                              PARTITION BY rents.home_id
                              ORDER BY rents.created_at DESC
                          ) AS last_rent
                     FROM mongo_rental_months AS rental_months
                          LEFT JOIN mongo_rents As rents
                          ON rental_months.rent_id = rents._id

                          LEFT JOIN mongo_users AS users
                          ON rents.user_id = users._id

                          LEFT JOIN mongo_rent_contracts AS rent_contracts
                          ON rents._id = rent_contracts.rent_id
                    WHERE rental_months."month" IS NOT NULL
                      AND (rents._status != ALL(ARRAY['canceled', 'finished_early', 'negotiation'])
                       OR rental_months._status != 'pending')
                      AND _charge_type != 'advance'
                      AND rental_months._status != 'canceled'
                      AND TO_DATE('01 ' || rental_months.month || ' ' || rental_months.year, 'DD Month YYYY') = DATE_TRUNC('month', CURRENT_DATE)
                      AND rent_contracts.signed_at IS NOT NULL
                  )
            WHERE last_rent = 1
),
meetings AS (SELECT home_meetings.home_id,
                    COALESCE(SUM(CASE
                                 WHEN home_meetings._step IN ('confirmed', 'canceled') THEN 1
                                 END), 0) AS citas_totales,
                    COALESCE(SUM(CASE
                                 WHEN home_meetings._step = 'confirmed' THEN 1
                                 END), 0) AS citas_confirmadas,
                    COALESCE(SUM(CASE
                                 WHEN home_meetings._step = 'canceled' THEN 1
                                 END), 0) AS citas_canceladas
               FROM mongo_home_home_meetings AS home_meetings
                    LEFT JOIN last_approved_by
                    ON home_meetings.home_id = last_approved_by.home_id
                       AND CONVERT_TIMEZONE('America/Mexico_City', home_meetings.created_at) >= last_approved_by.last_published_at
              GROUP BY home_meetings.home_id
),
locations AS (SELECT mongo_homes_id,
                     MIN(value) AS longitude,
                     MAX(value) AS latitude
                FROM mongo_homes_location
               GROUP BY mongo_homes_id
)
SELECT homes._id AS "ID Inmueble",
       homes._apartment_type AS "Tipo de inmueble",
       homes.product_type AS "Producto",
       homes.short_url AS "Link",
       homes.is_showed_by_homie AS "Mostrado por Homie",
       homes.virtual_tour_link AS "Tour Virtual",
       homes.door_keys_code AS "Codigo de Llaves",
       CASE
       WHEN rented.home_id IS NOT NULL
            THEN 'Rentado'
       WHEN rented.home_id IS NULL AND homes._status IN ('rented', 'reserved', 'master')
            THEN 'Baja'
       WHEN rented.home_id IS NULL AND homes._status ='in_progress'
            THEN 'En progreso'
       WHEN rented.home_id IS NULL AND homes._status = 'published'
            THEN 'Publicado'
       WHEN rented.home_id IS NULL AND homes._status = ANY (ARRAY ['temporarily_canceled_by_posposal', 'unpublished_by_meetings'])
            THEN 'Baja por citas'
       WHEN rented.home_id IS NULL AND homes._status = 'rejected'
            THEN 'Rechazado'
       WHEN rented.home_id IS NULL AND homes._status IN ('temporarily_canceled', 'unpublished', 'canceled')
            THEN 'Baja temporal'
       WHEN rented.home_id IS NULL AND homes._status = 'pending_approval'
            THEN 'Pendiente por aprobar'
       END AS "Status",
       CONVERT_TIMEZONE('America/Mexico_City', first_pending_approval.created_at) AS "Fecha Primer Pendiente Por Aprobar",
       CONVERT_TIMEZONE('America/Mexico_City', last_pending_approval.created_at) AS "Fecha Ultimo Pendiente Por Aprobar",
       CONVERT_TIMEZONE('America/Mexico_City', owners.created_at) AS "Registro del Propietario",
       CASE
       WHEN CONVERT_TIMEZONE('America/Mexico_City', owners.created_at) > first_approved_by.first_published_at THEN 'Si'
       ELSE 'No'
       END AS "Cambio Propietario?",
       user_owners._id AS "Id de Propietario",
       INITCAP(NVL(user_owners.name, '') || ' ' || NVL(user_owners.last_name, '') || ' ' || NVL(user_owners.second_last_name, '')) AS "Nombre del propietario",
       user_owners.email AS "Email del Propietario",
       user_owners.mobile_phone AS "Telefono del Propietario",
       rented.nombre_inquilino AS "Nombre del Inquilino",
       rented.email_inquilino AS "Email del Inquilino",
       rented.telefono_inquilino AS "Telefono del Inquilino",
       INITCAP(NVL(users_home_advisors.name, '') || ' ' || NVL(users_home_advisors.last_name, '') || ' ' || NVL(users_home_advisors.second_last_name, '')) AS "Nombre del Asesor Inmobiliario",
       users_home_advisors.email AS "Email del Asesor Inmobiliario",
       users_home_advisors.mobile_phone AS "Telefono Asesor Inmobiliario",
       CASE
       WHEN homes.address_from_location IS NULL
            THEN NVL(homes.street, ' ') || ' ' || NVL(homes.number, ' ') || ' ' || NVL(homes.neighborhood, ' ') || ' ' || NVL(homes.state, ' ') || ' ' || NVL(homes.country, ' ')
       ELSE homes.address_from_location
       END AS "Direccion",
       homes.created_at AS "Fecha de Creacion del Inmueble",
       first_approved_by.first_published_at AS "Fecha de primer publicación",
       last_approved_by.last_published_at AS "Fecha de última publicacion",
       CASE
       WHEN homes._status = 'unpublished' THEN 'Baja temporal (Homie)'
       WHEN homes._status IN ('canceled', 'temporarily_canceled') THEN 'Baja temporal (Propietario)'
       END AS "Tipo de Baja",
       unpublished_at.created_at AS "Fecha de Baja",
       SPLIT_PART(SPLIT_PART(unpublished_at.reason, '. Razón: ', 2), ': ', 1) AS "Razon de Baja",
       SPLIT_PART(SPLIT_PART(unpublished_at.reason, '. Razón: ', 2), ': ', 2) AS "Comentarios de Baja",
       bookeed_apartments.booked_date AS "Fecha del Apartado",
       bookeed_apartments.amount_booked AS "Monto del Apartado",
       CASE
       WHEN rented.home_id IS NOT NULL AND last_approved_by.last_published_at < rented.created_at
            THEN DATEDIFF(DAY, last_approved_by.last_published_at, rented.created_at)
       WHEN rented.home_id IS NOT NULL AND rented.created_at > last_approved_by.last_published_at
            THEN DATEDIFF(DAY, rented.created_at, last_approved_by.last_published_at)
       WHEN rented.home_id IS NULL AND homes._status IN ('rented', 'reserved', 'master')
            THEN DATEDIFF(DAY, unpublished_at.created_at, last_approved_by.last_published_at)
       WHEN rented.home_id IS NULL AND homes._status = 'in_progress'
            THEN DATEDIFF(DAY,GETDATE(), GETDATE())
       WHEN rented.home_id IS NULL AND homes._status = 'published'
            THEN DATEDIFF(DAY, last_approved_by.last_published_at, GETDATE())
       WHEN rented.home_id IS NULL AND homes._status = 'rejected'
            AND last_approved_by.last_published_at IS NULL
            THEN DATEDIFF(DAY, last_approved_by.last_published_at, unpublished_at.created_at)
       WHEN rented.home_id IS NULL AND homes._status IN ('temporarily_canceled', 'unpublished', 'canceled')
            THEN DATEDIFF(DAY, last_approved_by.last_published_at, unpublished_at.created_at)
       WHEN rented.home_id IS NULL AND homes._status = 'pending_approval'
            THEN DATEDIFF(DAY, GETDATE(), GETDATE())
       ELSE 0
       END AS "Días en Línea",
       CASE
       WHEN CONVERT_TIMEZONE('America/Mexico_City', homes.available_again_from) >= last_approved_by.last_published_at
            THEN CONVERT_TIMEZONE('America/Mexico_City', homes.available_again_from)
       END AS "Prox. Publicacion",
       CASE
       WHEN CONVERT_TIMEZONE('America/Mexico_City', homes.available_again_from) >= GETDATE()
            THEN DATEDIFF(DAY, CURRENT_DATE, CONVERT_TIMEZONE('America/Mexico_City', homes.available_again_from))
       END AS "Días Faltante Prox. Publicacion",
       homes.real_state_development_name AS "Desarrollo Inmobiliario",
       sh_sells.super_heroe AS "Asesor Adquisiciones",
       sh_customer_success.super_heroe AS "Asesor Customer Success",
       COALESCE(meetings.citas_totales, 0) AS "Citas Totales",
       COALESCE(meetings.citas_confirmadas, 0) AS "Citas Confirmadas",
       COALESCE(meetings.citas_canceladas, 0) AS "Citas Canceladas",
       homes.visits AS "Visitas",
       CASE
       WHEN filter_data_published.reactivated_at IS NOT NULL THEN 'Si'
       ELSE 'No'
       END AS "Reactivado",
       users_first_approved.email AS "Email agente 1er aprobacion",
       users_last_approved.email AS "Email agente Última aprobación",
       homes.campaign AS "Campaña",
       previous_price_report.previous_price AS "Precio anterior",
       COALESCE(homes.price, 0) + COALESCE(homes.extra_services_amount, 0) AS "Precio Total" ,
       COALESCE(homes.extra_services_amount, 0) AS "Mantenimiento",
       previous_price_report.change_date AS "Fecha de Cambio Precio",
       COALESCE(recommended_price.recommended_price, 0) AS "Precio Recomendado",
       homes._step AS "Paso",
       CASE
       WHEN homes.is_homie_exclusive IS NOT NULL AND homes.is_homie_exclusive = TRUE THEN 'Si'
       ELSE 'No'
       END AS "Inmueble exclusivo?",
       homes.bedrooms AS "Recamaras",
       homes.bathrooms AS "Baños",
       CASE
       WHEN homes.parkings IS NULL THEN 0
       ELSE homes.parkings
       END AS "Estacionanmientos",
       homes.sqare_mts AS "Area de Cobertura",
       homes.antique AS "Antigüedad",
       CASE
       WHEN (home_features.extras & (1 << 5)) > 0 THEN 'Si'
       ELSE 'No'
       END AS "Amueblado",
       CASE
       WHEN (home_features.rules & (1 << 0)) > 0 THEN 'Si'
       ELSE 'No'
       END AS "Acepta Mascotas",
       CASE
       WHEN homes.youtube_link IS NULL THEN 'No'
       WHEN homes.youtube_link ~~ 'https://%' THEN 'Si'
       ELSE 'No'
       END AS "Videorecorrido",
       CASE
       WHEN homes.has_external_photoshooting = TRUE THEN 'Si'
       ELSE 'No'
       END AS "Sesion Fotografica",
       homes.neighborhood AS "Colonia",
       homes.city AS "Ciudad",
       homes.state AS "Estado",
       homes.zip AS "Codigo Postal",
       CASE
       WHEN homes.is_rent_billable = TRUE THEN 'Si'
       ELSE 'No'
       END AS "Factura?",
       locations.longitude AS "Longitud",
       locations.latitude AS "Latitud"
  FROM mongo_homes AS homes
       LEFT JOIN rented
       ON homes._id = rented.home_id

       LEFT JOIN first_approved_by
       ON homes._id = first_approved_by.home_id

       LEFT JOIN mongo_users AS users_first_approved
       ON first_approved_by.changed_by_id = users_first_approved._id

       LEFT JOIN last_approved_by
       ON homes._id = last_approved_by.home_id

       LEFT JOIN mongo_users AS users_last_approved
       ON last_approved_by.changed_by_id = users_last_approved._id

       LEFT JOIN mongo_owners AS owners
       ON homes.owner_id = owners._id

       LEFT JOIN mongo_users AS user_owners
       ON (owners.user_id = user_owners._id)

       LEFT JOIN super_heroe AS sh_customer_success
       ON sh_customer_success.home_id =  homes._id
          AND sh_customer_success._super_hero_type = 'customer_success'

       LEFT JOIN super_heroe AS sh_sells
       ON sh_sells.home_id =  homes._id
          AND sh_sells._super_hero_type = 'sells'

       LEFT JOIN meetings
       ON meetings.home_id = homes._id

       LEFT JOIN filter_data_published_for_at_least_2month AS filter_data_published
       ON homes._id = filter_data_published.home_id
          AND last_approved_by.last_published_at = filter_data_published.reactivated_at

       LEFT JOIN mongo_homes_recommended_price AS recommended_price
       ON recommended_price.mongo_homes_id = homes.id

       LEFT JOIN bookeed_apartments
       ON homes._id = bookeed_apartments.home_id
          AND bookeed_apartments.booked_date >= last_approved_by.last_published_at

       LEFT JOIN mongo_home_features AS home_features
       ON homes._id = home_features.home_id

       LEFT JOIN unpublished_at
       ON homes._id = unpublished_at.home_id

       LEFT JOIN mongo_home_advisors AS home_advisors
       ON homes.advisor_id = home_advisors._id

       LEFT JOIN previous_price_report
       ON homes._id = previous_price_report.home_id

       LEFT JOIN mongo_users AS users_home_advisors
       ON users_home_advisors._id = home_advisors.user_id

       LEFT JOIN locations
       ON homes.id = locations.mongo_homes_id

       LEFT JOIN last_pending_approval
       ON last_pending_approval.home_id = homes._id

       LEFT JOIN first_pending_approval
       ON first_pending_approval.home_id = homes._id;
