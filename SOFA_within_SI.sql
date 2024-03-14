DROP MATERIALIZED VIEW IF EXISTS mimiciii_sepsislabels.SOFA_within_SI CASCADE;
CREATE MATERIALIZED VIEW mimiciii_sepsislabels.SOFA_within_SI AS
select si.hadm_id
, SOFA
, SOFAresp
, SOFAcoag
, SOFAliv
, SOFAcardio
, SOFAgcs
, SOFAren
, so.hlos as h_from_admission
, ha.admittime + so.hlos * interval '1 hour' as sepsis_time
from mimiciii_sofa.SOFAperhour so
join mimiciii_si.SI_flag si
on si.hadm_id = so.hadm_id
left join admissions ha
on ha.hadm_id = so.hadm_id
where  ha.admittime + so.hlos * interval '1 hour' between si_start - interval '1 hour' and si_end
and so.hlos>= 0
order by hadm_id, sepsis_time
;