WITH extra_features AS(

  SELECT postgres_rents_id,
         LISTAGG(value,', ') WITHIN GROUP (ORDER BY value) AS extra_features
    FROM webscraping.postgres_rents_extra_features
   GROUP BY postgres_rents_id
)
SELECT rents.id,
       rents.date_in AS created_at,
       rents.site,
       rents_response.title,
       rents_response.content AS description,
       rents.url,
       rents.state,
       rents.city,
       rents.neighborhood,
       rents.price,
       rents.price_maintenance,
       rents.covered_area,
       rents.land_area,
       rents.rooms,
       rents.bathrooms,
       rents.parking,
       rents.half_bathrooms,
       rents.antiquity,
       rents.price_x_mt2,
       rents.post_age,
       rents_response.pictures,
       rents.latitude,
       rents.longitude,
       extra_features.extra_features
  FROM webscraping.postgres_rents AS rents
       LEFT JOIN webscraping.postgres_rents_response AS rents_response 
       ON rents.id = rents_response.postgres_rents_id
       
       LEFT JOIN extra_features 
       ON rents.id = extra_features.postgres_rents_id
 WHERE rents.state IS NOT NULL
   AND rents.city IS NOT NULL
   AND rents.longitude IS NOT NULL
   AND rents.latitude IS NOT NULL
   AND ABS(rents.latitude) <= 90
   AND ABS(rents.longitude) <= 180
 ORDER BY rents.date_in DESC;