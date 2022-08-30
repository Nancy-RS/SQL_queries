WITH last_applications AS (SELECT home_application_id,
                                  risk_deposit_percentage_no_deposit
                             FROM (SELECT home_applications._id AS home_application_id,
                                          reports.risk_deposit_percentage_no_deposit,
                                          ROW_NUMBER () OVER (
                                              PARTITION BY reports.home_application_id
                                              ORDER BY reports.home_application_id, reports.created_at DESC) AS applications
                                     FROM mongo_home_applications AS home_applications
                                          INNER JOIN mongo_reports AS reports
                                          ON reports.home_application_id = home_applications._id
                                  )
                            WHERE applications = 1
)
SELECT rents._id AS "Id de Renta",
       INITCAP(NVL(users.name, ' ') || ' ' || NVL(users.last_name, ' ') || ' ' || NVL(users.second_last_name, ' ')) AS "Nombre del Inquilino",
       users.email AS "Email del Inquilino",
       homes._id AS "Id del Inmueble",
       last_applications.home_application_id AS "Id de Aplicacion",
       CASE
       WHEN homes.address_from_location IS NULL THEN NVL(homes.street, ' ') || ' ' || NVL(homes.number, ' ') || ' ' || NVL('Piso ' + homes.apartment_floor, ' ') || ' ' || NVL('Depa ' + homes.apartment_number, ' ') || ' ' || NVL(homes.neighborhood, ' ')|| ' ' || NVL(homes.city, ' ') || ' ' || NVL(homes.state, ' ') || ' ' || NVL(homes.country, ' ')
       ELSE homes.address_from_location
       END AS "Direccion",
       INITCAP(NVL(user_owners.name, ' ') || ' ' || NVL(user_owners.last_name, ' ') || ' ' || NVL(user_owners.second_last_name, ' ')) AS "Nombre del Propietario",
       CASE
       WHEN rents.parent_rent_id IS NOT NULL THEN TRUE
       END AS "Es Renovacion",
       CASE
       WHEN rents._id = parent_rents_id.parent_rent_id
            AND contracts.signed_at IS NOT NULL THEN TRUE
       END AS Renovo,
       rents.rent_price AS "Precio de Renta",
       rents._status AS "Status de Renta",
       CASE
       WHEN rental_months._charge_type = 'advance'
            AND rental_months._status = 'paid'
            AND rents._status = 'canceled' THEN CONVERT_TIMEZONE('America/Mexico_City', rents.canceled_at)
       END AS "Fecha de Cancelacion del Apartado",
       CONVERT_TIMEZONE('America/Mexico_City', rental_months.created_at) AS "Fecha de Apartado",
       contracts.started_at AS "Fecha de Inicio",
       contracts.ends_at AS "Fecha de Finalizacion",
       CASE
       WHEN rents._status = 'finished_early' THEN CONVERT_TIMEZONE('America/Mexico_City', rents.canceled_at)
       END AS "Fecha de Terminacion Anticipada",
       rents.reason_for_cancelation AS "Razon de cancelacion",
       CASE
       WHEN rents._status = 'started' THEN DATEDIFF(month, contracts.started_at, CURRENT_DATE)
       WHEN rents._status IN ('finished_early', 'canceled') THEN DATEDIFF(month, contracts.started_at, rents.canceled_at)
       WHEN rents._status = 'finished' THEN DATEDIFF(month, contracts.started_at, contracts.ends_at)
       END AS "Meses Transcurridos",
       homes.payment_day AS "Dia de Pago",
       CASE
       WHEN rents.is_no_deposit IS TRUE THEN guarantee_deposit.days
       ELSE rents.deposit_months_warranty * 30
       END AS "Dias De Deposito en Garantia",
       rents.deposit_months_warranty AS "Meses de Deposito en Garantia",
       rents.deposit_amount AS "Monto de Deposito en Garantia",
       rents.risk_guarantee AS "Poliza Juridica",
       guarantee_deposit.name AS "Tipo de Riesgo",
       CASE
       WHEN last_applications.risk_deposit_percentage_no_deposit IS NOT NULL THEN last_applications.risk_deposit_percentage_no_deposit
       WHEN rents.is_no_deposit IS TRUE THEN 0.3333
       END AS "Riesgo Homie",
       rents.homie_first_fee AS "Comision Primer Mes Homie",
       rents.homie_monthly_fee AS "Comision Mensual Homie",
       CASE
       WHEN rents.rent_third_party_company_fee_id IS NOT NULL THEN business_partners.name
       ELSE 'No'
       END AS "Asesor/Agencia",
       CASE
       WHEN rents.rent_third_party_company_fee_id IS NULL THEN advisor_fees.advisor_first_fee
       ELSE company_fees.company_first_fee
       END AS "Comision Primer Mes Asesor/Agencia",
       homes.product_type AS Producto
  FROM mongo_rents AS rents
       LEFT JOIN mongo_rent_contracts AS contracts
       ON contracts.rent_id = rents._id

       LEFT JOIN mongo_guarantee_deposit_schemes AS guarantee_deposit
       ON rents.guarantee_deposit_scheme_id = guarantee_deposit._id

       LEFT JOIN mongo_rent_third_party_company_fees AS company_fees
       ON company_fees._id = rents.rent_third_party_company_fee_id

       LEFT JOIN mongo_agency_business_partners AS business_partners
       ON business_partners._id = company_fees.fee_third_party_company_id

       LEFT JOIN mongo_homes AS homes
       ON rents.home_id = homes._id

       LEFT JOIN mongo_users AS users
       ON rents.user_id = users._id

       LEFT JOIN mongo_home_advisors AS home_advisors
       ON home_advisors.user_id = users._id

       LEFT JOIN mongo_rent_home_advisor_fees AS advisor_fees
       ON advisor_fees._id = home_advisors.home_advisor_fee_id

       LEFT JOIN mongo_owners AS owners
       ON homes.owner_id = owners._id

       LEFT JOIN mongo_users AS user_owners
       ON owners.user_id = user_owners._id

       LEFT JOIN mongo_rental_months AS rental_months
       ON rents._id = rental_months.rent_id
          AND rental_months._charge_type = 'advance'
          AND rental_months._status = 'paid'

       LEFT JOIN mongo_rents AS parent_rents_id
       ON parent_rents_id.parent_rent_id = rents._id
          AND contracts.rent_id = parent_rents_id.parent_rent_id

       LEFT JOIN last_applications
       ON last_applications.home_application_id = rents.home_application_id
 ORDER BY rents.created_at DESC;
