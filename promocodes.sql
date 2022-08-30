SELECT code AS Codigo,
       created_at AS "Fecha de Creación",
       due_date AS "Fecha de Vencimiento",
       agency AS Agencia,
       is_used AS "Es usado",
       used_times AS "Cantidad de usos",
       LISTAGG(usos, ' / ') AS Usos
  FROM (SELECT promocodes.code,
               CONVERT_TIMEZONE('America/Mexico_City', promocodes.created_at) AS created_at,
               CONVERT_TIMEZONE('America/Mexico_City', promocodes.due_date) AS due_date,
               CASE
               WHEN promocodes.agency_id IS NOT NULL THEN agencies.name
               ELSE 'homie'
               END AS agency,
               promocodes.is_used,
               CASE
               WHEN promocodes.is_used IS TRUE THEN promocodes.used_times
               ELSE 0
               END AS used_times,
               ROW_NUMBER() OVER (
                   PARTITION BY promocodes.code
                   ORDER BY promocodes_logs.used_at)
               || '.- ' || CONVERT_TIMEZONE('America/Mexico_City', promocodes_logs.used_at)
               || ', ' || promocodes_logs.home_application_id AS usos
          FROM mongo_promotion_codes AS promocodes
               LEFT JOIN mongo_agencies AS agencies
               ON agencies._id = promocodes.agency_id

               LEFT JOIN mongo_promotion_code_logs AS promocodes_logs
               ON promocodes_logs.promotion_code_id = promocodes._id)
 GROUP BY code, created_at, due_date, agency, is_used, used_times
 ORDER BY created_at DESC;
