WITH homes_by_owners AS (SELECT owner_id,
                                COUNT(*) AS homes,
                                SUM(CASE
                                    WHEN _status = 'published' THEN 1
                                    ELSE 0
                                    END) AS published_homes
                           FROM mongo_homes
                          GROUP BY owner_id
),

super_heroes AS (SELECT mongo_users_id,
                        email,
                        ROW_NUMBER () OVER(
                            PARTITION BY mongo_users_id
                        ) AS sh_ordering
                   FROM mongo_users_super_heroes
                  WHERE _super_hero_type = 'sells'
)

SELECT owners._id AS "Id de Propietario",
       CONVERT_TIMEZONE('America/Mexico_City', owners.created_at) AS "Fecha de Registro",
       INITCAP(nvl(users.name,'')||' '||nvl(users.last_name,'')||' '||
               nvl(users.second_last_name,'')) AS "Nombre del Propietario",
       users.mobile_phone AS "Telefono Propietario",
       users._person_type AS "Tipo de Persona",
       users.campaign AS "Campaña",
       owners.marketing_source AS "Marketing Source",
       users.referred_by_code AS "Código Referido",
       homes_by_owners.homes AS Inmuebles,
       homes_by_owners.published_homes AS "Inmuebles Publicados",
       super_heroes.email AS "Email Asesor de Adquisiciones"
  FROM mongo_owners AS owners
       LEFT JOIN mongo_users AS users
       ON owners.user_id = users._id

       LEFT JOIN homes_by_owners
       ON homes_by_owners.owner_id = owners._id

       LEFT JOIN super_heroes
       ON users.id = super_heroes.mongo_users_id
          AND super_heroes.sh_ordering = 1
 ORDER BY CONVERT_TIMEZONE('America/Mexico_City', owners.created_at) DESC;
