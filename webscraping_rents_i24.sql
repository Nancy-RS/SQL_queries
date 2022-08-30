WITH extra_features AS(

  SELECT postgres_rents_i24_id,
         LISTAGG(value,', ') WITHIN GROUP (ORDER BY value) AS extra_features
    FROM webscraping.postgres_rents_i24_extra_features
   GROUP BY postgres_rents_i24_id
)
SELECT id,
       date_in AS created_at,
       title,
       description,
       url,
       state,
       city,
       neighborhood,
       price,
       price_maintenance,
       price_no_maint,
       covered_area,
       land_area,
       rooms,
       bathrooms,
       parking,
       half_bathrooms,
       antiquity,
       post_age,
       latitude,
       longitude,
       query_search,
       extra_features.extra_features
  FROM webscraping.postgres_rents_i24 AS rents
       LEFT JOIN extra_features 
       ON rents.id = extra_features.postgres_rents_i24_id
 WHERE state IS NOT NULL
   AND rents.city IS NOT NULL
   AND rents.longitude IS NOT NULL
   AND rents.latitude IS NOT NULL
   AND ABS(rents.latitude) <= 90
   AND ABS(rents.longitude) <= 180
   AND price_no_maint > 0
 ORDER BY date_in DESC;