WITH super_heroes AS (SELECT mongo_users_id,
                             name,
                             email
                        FROM (SELECT mongo_users_id,
                                     name,
                                     email,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY mongo_users_id
                                     ) AS sh_rank
                                FROM mongo_users_super_heroes
                               WHERE _super_hero_type = 'ops'
                        )
                       WHERE sh_rank = 1
), 
gmail_messages AS (SELECT email,
                          provider
                     FROM (SELECT email,
                                  provider,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY email
                                      ORDER BY created_at ASC
                                  ) AS email_rank
                             FROM mongo_google_gmail_messages
                            ORDER BY email, provider ASC
                     )
                    WHERE email_rank = 1
), 
tenants AS (SELECT DISTINCT user_id
              FROM (SELECT user_id
                      FROM mongo_home_home_meetings
                     UNION ALL
                    SELECT user_id
                      FROM mongo_home_applications
                     UNION ALL
                    SELECT user_id
                      FROM mongo_rents
                   )
)
SELECT users._id AS "Id de Usuario",
       CONVERT_TIMEZONE('America/Mexico_City', users.created_at) AS "Fecha de Registro",
       users.name || ' ' || users.last_name || ' ' || users.second_last_name AS "Nombre del Inquilino",
       users.email AS "Email del Inquilino",
       users.mobile_phone AS "Telefono Inquilino",
       users.phone AS "Telefono Alternativo",
       users._person_type AS "Tipo de Persona",
       users.degree	AS "Grado de Estudios",
       users.has_children AS hijos,
       users.marital_status	AS "Estado civil",
       users.country_of_birth AS "Pais Natal",
       users.date_of_birth AS "Fecha de Nacimiento",
       super_heroes.name AS "Asesor de Rentas",
       super_heroes.email AS "Email del Asesor de Rentas",
       COALESCE(gmail_messages.provider, 'Organico') AS "Nombre de Listado"
  FROM mongo_users AS users
  LEFT JOIN super_heroes 
  ON users.id = super_heroes.mongo_users_id

  LEFT JOIN gmail_messages
  ON users.email = gmail_messages.email

 INNER JOIN tenants 
  ON tenants.user_id = users._id
 ORDER BY users.created_at DESC;