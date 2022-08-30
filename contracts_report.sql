SELECT rent_contracts._id AS "Id de Contrato",
       CONVERT_TIMEZONE('America/Mexico_City',
                         rent_contracts.created_at) AS "Fecha Creacion Contrato",
       rent_contracts.signed_at AS "Fecha Firma de Contrato",
       rents.parent_rent_id AS "Id Renta Previa",
       rents_parent._status AS "Status Renta Previa",
       rent_contracts.rent_id AS "Id de Renta",
       rent_contracts.started_at AS "Fecha de Inicio",
       rent_contracts.ends_at AS "Fecha de Finalización",
       rent_contracts._status AS "Status de Contrato",
       rents._status AS "Status de Renta",
       CASE
       WHEN rents.parent_rent_id IS NOT NULL THEN 'Si'
       ELSE 'No'
       END AS "Renovación",
       CASE
       WHEN rents.parent_rent_id IS NOT NULL
            AND rent_contracts.signed_at IS NOT NULL THEN 'Firmado'
       WHEN rents.parent_rent_id IS NOT NULL
            AND rent_contracts.signed_at IS NULL
            AND rent_contracts._status IN ('not_renewed', 'canceled') THEN 'No Renovado'
       WHEN rents.parent_rent_id IS NOT NULL
            AND rent_contracts.signed_at IS NULL
            AND rent_contracts._status IN ('pending', 'renewal_process') THEN 'Pendiente de Firma'
       ELSE 'NA'
       END AS "Status de Contrato Renovado",
       CASE
       WHEN rents.parent_rent_id IS NULL
            AND rent_contracts.signed_at IS NOT NULL THEN 'Firmado'
       WHEN rents.parent_rent_id IS NULL
            AND rent_contracts.signed_at IS NULL AND rent_contracts._status = 'canceled' THEN 'Cancelado'
       WHEN rents.parent_rent_id IS NULL
            AND rent_contracts.signed_at IS NULL
            AND rent_contracts._status = 'pending' THEN 'Pendiente de Firma'
       ELSE 'NA'
       END AS "Status de Nuevo Contrato"
  FROM mongo_rent_contracts AS rent_contracts
       LEFT JOIN mongo_rents AS rents
       ON rents._id = rent_contracts.rent_id

       LEFT JOIN mongo_rents AS rents_parent
       ON rents_parent._id = rents.parent_rent_id
