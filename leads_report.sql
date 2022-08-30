WITH discarted_reasons AS (SELECT leads.id AS lead_id,
                                  LISTAGG(tags.name,' & ')
                                  WITHIN GROUP (ORDER BY leads.id) AS discarted_reason
                             FROM postgres_leads AS leads
                                  INNER JOIN postgres_taggings AS taggings
                                  ON taggings.taggable_id = leads.id
                                     AND taggings.context = 'lead_cancellations'

                                  LEFT JOIN postgres_tags AS tags
                                  ON tags.id = taggings.tag_id
                            GROUP BY leads.id
)
SELECT leads.id AS "Id Lead",
       INITCAP(COALESCE(leads.name, '') || ' ' || COALESCE(leads.last_name, '') || ' ' || COALESCE(leads.mother_last_name, '')) AS "Nombre del Inquilino",
       leads.phone "Telefono Inqulino",
       leads.mobile_phone AS "Telefono Alternativo",
       leads.email AS "Email del Inqulino",
       countries.name AS "Pais Natal",
       CONVERT_TIMEZONE('America/Mexico_City', leads.created_at) AS "Fecha Creacion Lead",
       leads.status AS "Status Lead",
       INITCAP(COALESCE(users.name, '') || ' ' || COALESCE(users.last_name, '') || ' ' || COALESCE(users.mother_last_name, '')) AS Representante,
       users.email AS "Email Representante",
       CASE
       WHEN discarded_at IS NOT NULL THEN TRUE
       ELSE FALSE
       END AS "Lead Archivado",
       CONVERT_TIMEZONE('America/Mexico_City', leads.discarded_at) AS "Fecha de Archivo",
       discarted_reasons.discarted_reason AS "Razon de Archivo"
  FROM postgres_leads AS leads
       LEFT JOIN postgres_users AS users
       ON leads.user_id = users.id

       LEFT JOIN postgres_countries AS countries
       ON leads.country_code = countries.id

       LEFT JOIN discarted_reasons
       ON discarted_reasons.lead_id = leads.id
          AND leads.discarded_at IS NOT NULL;
