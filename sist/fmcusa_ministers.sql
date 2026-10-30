WITH min_ids AS (
    SELECT DISTINCT 
        minister_id
    FROM conferenceMegMac
    WHERE 1=1
        AND minister_id IS NOT NULL
),

ms AS(
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY minister_id ORDER BY ministers_surveys.submission_date DESC) AS n
    FROM ministers_surveys
),

id_and_key AS(
    SELECT 
        *,
        (SELECT TOP 1 pk_conferenceMegMac 
        FROM conferenceMegMac 
        WHERE 1=1
            AND conferenceMegMac.minister_id = min_ids.minister_id 
            AND action_taken IN (SELECT action_taken FROM cog_megmac_actions WHERE action_type = 'MAC')
        ORDER BY [date] DESC
        ) AS appointment_id
    FROM min_ids
)

SELECT
    id_and_key.minister_id,
    COALESCE(ms.name_yearbook, cmm.minister_name) AS minister_name,
    CONCAT(
        COALESCE(ms.name_yearbook, cmm.minister_name),
        ' (',
        id_and_key.minister_id,
        ')'
    ) AS minister_name_and_id,
    CASE WHEN ms.phone_pub=1 THEN ms.phone ELSE 'private' END AS minister_phone,
    CASE WHEN ms.email_pub=1 THEN ms.email ELSE 'private' END AS minister_email,
    cmm.church_id AS church_id,
    churches.church_name AS church_name,
    cmm.action_taken AS recent_appointment
FROM id_and_key
LEFT JOIN conferenceMegMac AS cmm ON 1=1 
    AND cmm.PK_conferenceMegMac = id_and_key.appointment_id
LEFT JOIN ms ON 1=1
    AND ms.n = 1
    AND ms.minister_id = id_and_key.minister_id
LEFT JOIN churches ON 1=1
    AND churches.church_num = cmm.church_id
WHERE 1=1
    AND cmm.action_taken IS NOT NULL
ORDER BY id_and_key.minister_id ASC
;
