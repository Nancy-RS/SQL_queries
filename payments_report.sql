WITH payment_references AS (SELECT payment_references.rental_month_id,
                                   payment_references.apply_dispersion,
                                   payment_references.applied_to_rental_month,
                                   payment_references.store_name,
                                   payment_references.reference_description,
                                   payment_references._provider
                              FROM mongo_payment_reference_requests AS payment_references
                              LEFT JOIN mongo_rental_months AS rental_months
                                   ON rental_months._id = payment_references.rental_month_id
                             WHERE payment_references.applied_to_rental_month IS TRUE
),
rental_payments AS (SELECT rental_payments.rental_month_id,
                           CONVERT_TIMEZONE('America/Mexico_City', rental_payments.paid_at) AS payment_date,
                           rental_payments.payment_method,
                           rental_payments._status AS payment_status,
                           rental_payments.payment_code,
                           rental_payments.card_type,
                           rental_payments.card_brand,
                           rental_payments.extra_fee,
                           rental_payments.amount
                      FROM mongo_rental_payments AS rental_payments
                           LEFT JOIN mongo_rental_months AS rental_months
                           ON rental_payments.rental_month_id = rental_months._id
                     WHERE rental_payments._status = 'applied'
),
srpago_dispersions AS (SELECT rental_months._id AS rental_month_id,
                              SUM(owner_dispersions.amount_in_sr_pago) AS owner_amount_srpago,
                              SUM(advisor_dispersions.amount_in_sr_pago) AS advisor_amount_srpago,
                              SUM(homie_dispersions.amount_in_sr_pago) AS homie_amount_srpago,
                              SUM(srpago_dispersions.amount) AS srpago_amount
                         FROM mongo_rental_months AS rental_months
                              LEFT JOIN mongo_payment_reference_requests AS payment_references
                              ON payment_references.rental_month_id = rental_months._id

                              LEFT JOIN mongo_payment_reference_requests_dispersion_data AS dispersion_data
                              ON dispersion_data.mongo_payment_reference_requests_id = payment_references.id

                              LEFT JOIN mongo_payment_reference_requests_dispersion_data_owner AS owner_dispersions
                              ON owner_dispersions.mongo_payment_reference_requests_dispersion_data_id = dispersion_data.id

                              LEFT JOIN mongo_payment_reference_requests_dispersion_data_advisor AS advisor_dispersions
                              ON advisor_dispersions.mongo_payment_reference_requests_dispersion_data_id = dispersion_data.id

                              LEFT JOIN mongo_payment_reference_requests_dispersion_data_homie AS homie_dispersions
                              ON homie_dispersions.mongo_payment_reference_requests_dispersion_data_id = dispersion_data.id

                              LEFT JOIN mongo_payment_reference_requests_dispersion_data_sr_pago AS srpago_dispersions
                              ON srpago_dispersions.mongo_payment_reference_requests_dispersion_data_id = dispersion_data.id
                        WHERE payment_references.applied_to_rental_month IS TRUE
                        GROUP BY rental_months._id
),
rent_dispersions AS (SELECT rental_month_dispersions.rental_month_id,
                            rental_month_dispersions.amount,
                            rental_month_dispersions.email,
                            rental_month_dispersions._status,
                            rental_month_dispersions._applies_to
                       FROM mongo_rental_months AS rental_months
                            LEFT JOIN mongo_rental_month_dispersions AS rental_month_dispersions
                            ON rental_month_dispersions.rental_month_id = rental_months._id
),
interests AS (SELECT rental_month_id,
                     SUM(interests) AS interests_amount
                FROM mongo_interest_historics
               GROUP BY rental_month_id
)
SELECT rental_months._id AS "Id Renta Mensual",
       rents._id AS "Id de Renta",
       INITCAP(NVL(users.name, ' ') || ' ' || NVL(users.last_name, ' ')  || ' ' || NVL(users.second_last_name, ' ')) AS "Nombre del Inquilino",
       users.email AS "Email del Inquilino",
       user_owners.email AS "Email del Propietario",
       rental_payments.payment_date AS "Fecha de Pago",
       rental_months.due_date AS "Fecha de Vencimiento",
       CASE
       WHEN rental_months.month = 'january' THEN 'enero/' || rental_months.year
       WHEN rental_months.month = 'february' THEN 'febrero/' || rental_months.year
       WHEN rental_months.month = 'march' THEN 'marzo/' || rental_months.year
       WHEN rental_months.month = 'april' THEN 'abril/' || rental_months.year
       WHEN rental_months.month = 'may' THEN 'mayo/' || rental_months.year
       WHEN rental_months.month = 'june' THEN 'junio/' || rental_months.year
       WHEN rental_months.month = 'july' THEN 'julio/' || rental_months.year
       WHEN rental_months.month = 'august' THEN 'agosto/' || rental_months.year
       WHEN rental_months.month = 'september' THEN 'septiembre/' || rental_months.year
       WHEN rental_months.month = 'october' THEN 'octubre/' || rental_months.year
       WHEN rental_months.month = 'november' THEN 'noviembre/' || rental_months.year
       WHEN rental_months.month = 'december' THEN 'diciembre/' || rental_months.year
       ELSE rental_months.month || '/' || rental_months.year
       END AS "Mes/Año Pagado",
       CASE
       WHEN rental_months._charge_type = 'first_payment' THEN 'Primer Pago'
       WHEN rental_months._charge_type = 'rent_pay' THEN 'Renta Mensual'
       WHEN rental_months._charge_type = 'advance' THEN 'Apartado'
       END AS "Tipo de Cargo",
       rents.product AS Producto,
       CASE
       WHEN rental_months._status = 'paid' THEN 'Pagado'
       WHEN rental_months._status = 'refunded' THEN 'Reembolso'
       END AS "Status Renta Mensual",
       rents.rent_price AS "Precio de Renta",
       CASE
       WHEN rental_months._charge_type = 'advance' THEN rental_months.amount
       END AS "Monto de Apartado",
       CASE
       WHEN rental_months._charge_type = 'first_payment' THEN rents.risk_guarantee
       END AS "Poliza Juridica",
       CASE
       WHEN rental_months._charge_type = 'first_payment'
            AND (rents.is_no_deposit IS FALSE OR rents.is_no_deposit IS NULL) THEN FALSE
       END AS "Es No deposito",
       CASE
       WHEN rental_months._charge_type = 'first_payment' THEN rents.deposit_months_warranty
       WHEN rental_months._charge_type != 'first_payment' THEN NULL
       END AS "Meses en Deposito en Garantia",
       CASE
       WHEN rents.allow_monthly_owner_deposit_payment IS TRUE
            AND (DATEDIFF(month, rents.start_date, rental_months.due_date) < rents.months_to_pay_deposit
            OR rental_months._charge_type = 'first_payment') THEN TRUE
       ELSE FALSE
       END AS "Es deposito Diferido",
       CASE
       WHEN rents.allow_monthly_owner_deposit_payment IS TRUE
            AND (DATEDIFF(month, rents.start_date, rental_months.due_date) < rents.months_to_pay_deposit
            OR rental_months._charge_type = 'first_payment') THEN ROUND(rents.deposit_amount / rents.months_to_pay_deposit, 2)
       END AS "Deposito en Garantia",
       rental_months.homie_fee AS "Comision Homie (Sin IVA)",
       rental_payments.amount + rental_payments.extra_fee AS "Pago Recibido",
       rental_months.remaining_amount AS "Cantidad Faltante",
       rental_payments.payment_method AS "Metodo de Pago",
       CASE
       WHEN rental_payments.payment_status = 'applied' THEN 'Aplicado'
       WHEN rental_payments.payment_status = 'refunded' THEN 'Reembolsado'
       END AS "Status de Pago",
       rental_payments.payment_code AS "Codigo de Pago",
       CASE
       WHEN rental_payments.card_type IS NULL THEN 'No disponible'
       ELSE rental_payments.card_type
       END AS "Tipo de Tarjeta",
       CASE
       WHEN rental_payments.card_brand IS NULL THEN 'No disponible'
       ELSE rental_payments.card_brand
       END AS "Marca de Tarjeta",
       CASE
       WHEN interests.interests_amount IS NULL THEN 0
       ELSE interests.interests_amount
       END AS "Intereses generados",
       rental_payments.extra_fee AS "Comision por Metodo de Pago",
       payment_references._provider AS "Proveedor",
       payment_references.store_name AS "Tienda/Banco/Spei",
       payment_references.reference_description AS "Referencia",
       payment_references.apply_dispersion AS "Aplica Dispersion",
       ROUND(srpago_dispersions.owner_amount_srpago, 2) AS "Cantidad Propietario Sr Pago",
       ROUND(srpago_dispersions.advisor_amount_srpago, 2) AS "Cantidad Asesor Sr Pago",
       ROUND(srpago_dispersions.homie_amount_srpago, 2) AS "Cantidad Homie Sr Pago",
       ROUND(srpago_dispersions.srpago_amount, 2) AS "Cantidad Sr Pago",
       ROUND(owner_dispersions.amount, 2) AS "Dispersion Propietario",
       owner_dispersions.email AS "Email Dispersion Propietario",
       owner_dispersions._status AS "Status Dispersion Propietario",
       ROUND(advisor_dispersions.amount, 2) AS "Dispersion Asesor",
       advisor_dispersions.email AS "Email Dispersion Asesor",
       advisor_dispersions._status AS "Status Dispersion Asesor",
       ROUND(other_dispersions.amount, 2) AS "Otra Dispersion",
       other_dispersions.email AS "Email Otra Dispersion",
       other_dispersions._status AS "Status Otra Dispersion"
  FROM mongo_rental_months AS rental_months
       INNER JOIN mongo_rents AS rents
       ON rents._id = rental_months.rent_id

       LEFT JOIN mongo_users AS users
       ON users._id = rents.user_id

       LEFT JOIN mongo_homes AS homes
       ON homes._id = rents.home_id

       LEFT JOIN mongo_owners AS owners
       ON owners._id = homes.owner_id

       LEFT JOIN mongo_users AS user_owners
       ON user_owners._id = owners.user_id

       LEFT JOIN payment_references
       ON payment_references.rental_month_id = rental_months._id

       LEFT JOIN rental_payments
       ON rental_payments.rental_month_id = rental_months._id

       LEFT JOIN srpago_dispersions
       ON srpago_dispersions.rental_month_id = rental_months._id

       LEFT JOIN rent_dispersions AS advisor_dispersions
       ON advisor_dispersions.rental_month_id = rental_months._id
            AND advisor_dispersions._applies_to = 'advisor'

       LEFT JOIN rent_dispersions AS owner_dispersions
       ON owner_dispersions.rental_month_id = rental_months._id
            AND owner_dispersions._applies_to = 'owner'

       LEFT JOIN rent_dispersions AS other_dispersions
       ON other_dispersions.rental_month_id = rental_months._id
            AND other_dispersions._applies_to NOT IN ('advisor', 'owner')

       LEFT JOIN interests
       ON interests.rental_month_id = rental_months._id
 WHERE rental_months._status IN ('paid', 'refunded')
 ORDER BY rental_payments.payment_date DESC;
