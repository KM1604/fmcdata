CREATE PROCEDURE u_conferencemegmac_2025 AS

BEGIN

DELETE 
FROM conferenceMegMac
WHERE 1=1
    AND [date] >= DATEFROMPARTS(2025,1,1)
    AND [date] < DATEFROMPARTS(2026,1,1)
    AND comments NOT LIKE '%delete%'
;

INSERT INTO conferenceMegMac(
    [date],
    conference_name,
    conference_id,
    minister_name,
    minister_id,
    ordination_status,
    action_taken,
    role_title,
    organization_name,
    church_id,
    lead_pastor_yn,
    comments
    )

SELECT
    effective_date,
    conference_name,
    conference_id,
    minister_name,
    minister_id,
    ordination_status,
    action_taken,
    role_title,
    organization_name,
    church_id,
    lead_pastor_yn,
    comments
FROM s_conferencemegmac_2025
;

END