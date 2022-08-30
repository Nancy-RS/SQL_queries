WITH documents AS (SELECT home_application_id,
                          step,
                          MAX(created_at) AS step_date
                     FROM mongo_home_application_logs
                    WHERE step IN ('documents', 'complete')
                    GROUP BY home_application_id, step
)
SELECT application_id AS "Id de Aplicacion",
       home_id AS "Id del Inmueble",
       created_at AS "Fecha Creacion Aplicacion",
       _step AS "Paso de Aplicacion",
       product AS Producto,
       tenant_name AS "Nombre del Inquilino",
       tenant_email AS "Email del Inquilino",
       tenant_mobile_phone AS "Telefono Inquilino",
       age AS Edad,
       marital_status AS "Estado Civil",
       income_amount AS Ingresos,
       date_of_birth AS "Fecha de Nacimiento",
       occupation AS Ocupacion,
       company_name AS "Empresa donde Labora",
       seniority_years AS "Tiempo en la Empresa",
       gender AS Genero,
       address AS Direccion,
       _status AS "Status de Aplicacion",
       homie_score AS "Homie Score",
       documented_at AS "Fecha Entrega de Documentos",
       completed_at AS "Fecha de Aplicacion Completada",
       resolved_at AS "Fecha Respuesta Aplicacion",
       approved_applications AS "Aplicaciones Aprobadas",
       paid_applications AS "Aplicaciones Pagadas",
       paid_at AS "Fecha Pago Aplicacion",
       Roomies,
       _person_type AS "Tipo de Persona",
       Fico,
       Promocode,
       percentage_discount AS "Porcentaje Descuento",
       is_candidate_no_deposit AS "Candidato a No deposito",
       homie_risk AS "Tipo de Riesgo"
  FROM (SELECT applications._id AS application_id,
               applications.home_id AS home_id,
               CONVERT_TIMEZONE('America/Mexico_City', applications.created_at) AS created_at,
               applications._step,
               rents.product,
               INITCAP(NVL(users.name, ' ') || ' ' || NVL(users.last_name, ' ') || ' ' ||
                       NVL(users.second_last_name, ' ')) AS tenant_name,
               users.email AS tenant_email,
               users.mobile_phone AS tenant_mobile_phone,
               DATEDIFF('year', users.date_of_birth, CURRENT_TIMESTAMP::DATE) AS age,
               users.marital_status,
               incomes.verified_income AS income_amount,
               users.date_of_birth::DATE,
               incomes.position AS occupation,
               incomes.working_company AS company_name,
               incomes.seniority_years,
               CASE
               WHEN users._gender = 'male' THEN 'Hombre'
               WHEN users._gender = 'female' THEN 'Mujer'
               ELSE 'Sin Registro'
               END AS gender,
               CASE
               WHEN homes.address_from_location IS NULL
                    THEN NVL(homes.street, ' ') || ' ' || NVL(homes.number, ' ') || ' ' || NVL(homes.neighborhood, ' ')
               ELSE homes.address_from_location
               END AS address,
               applications._status,
               CASE
               WHEN applications._status = 'approved' THEN reports.homie_score
               END AS homie_score,
               CONVERT_TIMEZONE('America/Mexico_City', send_documents.step_date) AS documented_at,
               CONVERT_TIMEZONE('America/Mexico_City', complete_documents.step_date) AS completed_at,
               CONVERT_TIMEZONE('America/Mexico_City', applications.resolved_at) AS resolved_at,
               CASE
               WHEN applications._status = 'approved' THEN 1
               ELSE 0
               END AS approved_applications,
               CASE
               WHEN payments.paid_at IS NOT NULL THEN 1
               ELSE 0
               END AS paid_applications,
               CONVERT_TIMEZONE('America/Mexico_City', payments.paid_at) AS paid_at,
               CASE
               WHEN incomes.number_of_homies IS NULL THEN 0
               ELSE incomes.number_of_homies
               END AS Roomies,
               applications._person_type,
               applications.score AS Fico,
               promos.code AS Promocode,
               promos.percentage AS percentage_discount,
               applications.is_candidate_no_deposit,
               guarantee_deposit.name AS homie_risk,
               ROW_NUMBER() OVER (
                   PARTITION BY applications._id, applications.agency_id
                   ORDER BY  applications._id,applications.agency_id, promos.created_at DESC
               ) AS order
          FROM mongo_home_applications AS applications
               LEFT JOIN mongo_rents AS rents
               ON rents.home_id = applications.home_id

               LEFT JOIN mongo_users AS users
               ON users._id = applications.user_id

               LEFT JOIN mongo_homes AS homes
               ON homes._id = applications.home_id

               LEFT JOIN mongo_home_applications_home_application_income AS incomes
               ON applications.id = incomes.mongo_home_applications_id

               LEFT JOIN mongo_payments AS payments
               ON payments.paymentable_id = applications._id

               LEFT JOIN mongo_promotion_codes AS promos
               ON promos.agency_id = applications.agency_id

               LEFT JOIN mongo_guarantee_deposit_schemes AS guarantee_deposit
               ON applications.guarantee_deposit_scheme_id = guarantee_deposit._id

               LEFT JOIN mongo_reports AS reports
               ON reports.home_application_id = applications._id

               LEFT JOIN documents AS send_documents
               ON send_documents.home_application_id = applications._id
                  AND send_documents.step = 'documents'

               LEFT JOIN documents AS complete_documents
               ON complete_documents.home_application_id = applications._id
                  AND complete_documents.step = 'complete'
         WHERE applications._step NOT IN ('previous_landlord', 'resume')
         ORDER BY applications.created_at DESC) AS application_data
 WHERE application_data.order = 1
 ORDER BY created_at DESC;
