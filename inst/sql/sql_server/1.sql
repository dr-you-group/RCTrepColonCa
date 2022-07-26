CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (36555575,36534885,36531732,36526893,36526623,36525214,36543691,36557132,36518465,36532893,36529793,36524507,36553857,36522964,36547074,36550422,36557152,36534051,36541764,36555420,36549710,44500410,44500863,44500874,44502935,44500411,44500245,44502529,36546337,36556767,36556262,36529455,44499436,36522002,36535150,36518315,44500353,36548040,36565190,44500060,36566260,36538847,44502946,44502705,44502776,44499892,44499550,44500141,44500596,44498956,44501728,44500792,44500026,44501145,44503020,44500198,36562519,36519060,36523630,36558447,36528366,36530984,36543695,36531184,36548113,36531299,36524117,36539303,36533201,36549024,44501524,44499890,36523302,36552722,44500413,36562328,36562377,44502717,44499654,36521693,36539545,36561654,36523927,36551546,36542672,36519618,36539328,36517480,36541105,36559504,36557136,36535623,36528720,36557689,36525408,36522352,36565894,36559232,44502439,44504337,44500497,44501932,44504380,44500496,44500927,42512838,1553182,42512803,44502094,36538491,1553487,42511611,44502398,44499565,36524059,36540653,44503097,36561240,44500082,36542212,36524627,36536240,36542695,36563888,36556079,36518071,36519383,36535881,36558757,36518346,36545407,36536957,36550745,36567509,36521890,36558695,36553659,36533823,36543039,36536302,36565374,36539817,36552398,36537640,36565937,36549611,36549219,36549968,36531207,36545243,36532722,36528202,36563618,36534494,36567879,36518249,36558981,36559174,36548337,36545850,36544204,36540841,36537744,36529464,36535941,36537123,36532354,36519097,36517866,36556415,36533199,36539463,36530880,36526576,36523057,36555013,36523408,36535070,36521864,36520432,36531565,36550318,36559658,36545685,36541845,36551038,36548686,36551850,36550549,36552850,36540026,36566568,36547834,36561404,36528389,36532881,36518792,36543589,36562126,36553054,36528813,36521307,36531949,36518448,36542121,36518916,36543954,36558313,36543602,36545158,44501400,36528999,44501436,36522717,36544716,36557134,36560507,36545304,36562794,36527031,36527707,36541841,36521973,36567869,36551281,36534455,36531573,36525291,36530070,36533511,36524229,36545002,36567934,36539322,36537785,36520709,36567564,36554334,36541351,44504349,44504336,44504373,44504355,44504379,44504367,44504361,36534602,36553697,36567938,36527398,36534469,36560599,36524579,36531345,36555504,36560775,36543319,36542446,36538960,36560731,36548322,36547476,36543160,36549206,36539064,36533090,44502074,36539283,36542051,36547508,36520412,36548476,36544256,36521999,36566125,36535310,36539114,36531708,36542293,36526908,36517782,36521726,42511969,42511708,42512593,36560961,36561218,36526256,36518878,36568007,36525365,36557661,44500018,36556197,36536335,36540380,36548330,36560788,36544782,36540392,36524903,36538962,36531825,36560308,36556738,36563299,36565463,36518766,36549192,36519255,36543236,36566901,36565069,36536321,44499609,44500225,44503127,36530133,44502752,44503126,44500560,42512946,36568351,36568226,36568291,36524496,36517374,36558375,36556198,36558850,36561901,36566642,36540047,36526153,36518247,36554057,36566228,36558147,36519246,36529294,36552278,36565009,36531547,36517926,36530098,36554347,36526487,36557666,36567253,44503167,44501742,36552564,36528559,44501224,36519365,36541290,36536176,36548757,36550771,36528217,36537464,36522380,36556087,36561054,36566374,36561337,36532584,36567377,36564197,36519953,36551880,36520347,36559043,36548807,36567280,36559280,36551309,36557384,36520261,36560012,36549609,36517472,36526206,36555302,36555555,36526690,36530243,36548799,36532436,36543321,36548741,36565386,36541403,36519842,36529329,36544638,36538072,36529508,36520727,36551485,36548318,36543148,36537862,36556504,36542858,36532352,36555725,36540023,44502668,36527298,36554395,36520329,36519446,44501460,36539731,44501437,36537968,36541809,36530312,36535977,36560525,36523957,36550344,36543585,36549982,36557428,36527599,36532769,36550796,44499930,36522659,36520829,44502351,36522868,44502130,36525988,36522394,36517381,36543543,36524969,36560037,36534477,36530653,36530931,36565751,36535494,36531202,36520266,36562617,36567417,36543066,36565123,36546439,36542470,36561728,36559258,36544152,36548145,36541312,36535559,36553110,36521098,36526783,36543343,36564888,36523773,36564791,36539609,36537310,36554391,36539129,36534718,36524467,36544523,36565934,36521519,36546262,36557202,36565575,36521548,36531685,36558010,36549033,36524678,36554885,36560992,36549018,36531068,36564249,36542943,36562655,36530841,36559490,36523931,36553795,36523257,36522296,36538282,36549301,36555113,36550918,36546976,36561602,36553101,36542599,36523438,36554328,36562499,36522314,36550297,36549194,36549996,36557473,36565873,36551907,36553308,36560235,36562429,36530846,36532654,36520385,36531048,36560675,36538401,36517448,36528505,36544273,36567190,36564471,36556219,36564805,36537407,36528729,36523986,36566181,36523356,36536742,44500279,36554421,36523885,36517956,36551962,36554220,36537545,36537467,36522981,36552412,36521000,36559900,36563145,36540996,36529610,36521711,36524379,36543889,36554253,36539535,36523073,36563545,36562958,36544767,36551422,36559200,36524376,36564724,36525017,36518804,36540852,36554482,36535698,36556763,36527743,36557476,36546813,36566753,36552921,36542217,36557269,36537396,36530121,36562834,36535622,36519032,36550536,36547105,36531794,36566584,36529021,36538555,36524150,36528447,36546532,36541749,36534988,36519303,36526232,36540433,36525801,36535247,36526472,36563372,36552302,36532947,36550035,36539261,36537038,36566255,36528533,36550419,36519173,42512863,36537107,36531884,36535930,36548467,44502132,36552208,36564184,36524970,36529847,36544010,36517253,36547710,36531641,36541859,36519597,36518682,36537910,36519524,36536000,36519918,36533313,36540651,36566940,36567994,36563643,36552127,36556951,36526941,36520575,36532404,36557469,36536398,36550353,36521492,36545148,36531137,36542980,36551676,36528898,36563654,36536660,36566497,44502127,36547263,36533883,36528016,36556986,36551803,36533833,44501534,36567857,36522786,36557976,36517505,36519040,36552995,36547715,36518122,36548298,36563204,36522580,36563678,36561097,36530496,36547322,36533101,36522903,36544096,36556501,36555730,44500944,36532129,36539039,36532625,36539054,36545170,36565139,36530022,44499408,36549012,44503524,36535635,44502064,36537229,36545149,36518844,44501823,36553121,36529145,36520468,36524571,36522804,36527889,36518466,36539653,36542107,36549380,36517771,36536211,36565187,44501294,36547693,36547125,44503526,36519473,36535760,36527196,36541268,36518044,36555122,36566397,36535606,36561093,36565126,36519259,36535343,36541198,36522585,36566616,36548221,42512703,36561239,36518296,36531042,36524169,36554404,36552986,36545083,36546777,36517757,36558419,36528298,36527191,36540812,36540732,36537537,36520077,36535590,36539402,36556972,36561086,36522561,36564236,44502067,36558692,36536279,36522969,44503527,36544250,44499785,44500913,36566548,44502501,44501690,36534487,44499858,36566343,36550661,36562785,36527282,36549451,36546054,36539234,36539585,36556042,36521944,36565400,36565053,36544700,36550510,44500738,36530655,36526950,36517666,44502115,36534873,36531004,42512414,36519642,36561723,36537969,36528392,36526752,36567435,36563922,42512903,42512762,42511614,42512140,42511613,42511881,42512948,36554549,36524400,36523010,36528005,36550171,36560466,36553960,44501020,44501649,44501088,44500736,44502709,44502112,44501952,44501804,44502707,44499921,44503091,44502029,44502395,44498960,44501018,36524523,36521082,36527693,36557186,36539431,36519395,36563756,36518940,36549075,36549294,36542344,36539438,36553020,36549262,36553582,36543368,36548615,36548427,36535508,36563193,36560665,36556997,36517343,36524656,36538390,36554024,36535526,36540477,36540509,36541244,36517840,36565948,36563181,36549340,36521918,42512351,42511753,42512169,42511982,42511723,42512603,42512172,36525060,36527281,36519376,36549467,36558421,36538836,36565921,36558609,36553946,36540300,36564432,36564121,36518600,36558054,36539092,36554575,36534694,36530900,36535565,36531616,36524082,36563973,36537248,36526244,36552787,36525003,36545084,36518309,36543792,36530430,36543390,36554814,36535607,36534131,36530160,44504346,44504333,44504370,44504352,44504376,44504364,44504358,44502332,44499888,44503148,36559495,44501722,36558239,36543210,36550255,44500447,36553506,36564473,36560570,36567921,36552171,36566070,44501142,44502175,36546604,44500648,36528154,36534597,36522630,36519514,36524271,36523103,36531112,36545535,36560294,36553253,36533218,36518112,36518519,36565794,36533966,36562620,42511748,36539530,36522066,36547602,36553340,36531711,36556582,36526826,36528582,36518853,36547437,36545245,36530857,36540864,36548080,36538958,36546491,36550043,36535160,36561447,36567575,36548426,36532339,36544906,36519550,36519612,36537155,36560559,36550073,36536935,36520110,36534328,36560038,36524481,36562561,36533402,36541753,36529077,36531080,36526290,36549367,36531329,36552602,36535418,36552060,36563965,36545557,36564789,36518658,36526474,36541610,36546619,36563149,36548408,36556599,36566761,36521262,36567513,36556550,36529735,36544884,36541703,36522396,36520194,36555697,36537522,36550743,36550945,36536817,36543394,36518376,36533644,36563879,36559052,36543087,36537015,36518144,36545915,36552072,36559888,36518870,36522672,36561931,36536967,36517398,36525393,36559985,36548950,36536863,36532966,36563595,36530992,36538090,36554073,36552071,36518445,36529815,36520219,36530195,36539813,36538316,36543599,44501787,36534141,36560403,36548695,36554550,42512063,42512255,42512100,42512292,42512512,42512076,36565406,36558213,36554679,36537494,36523683,36520634,36553859,44499559,44503025,44501412,44503026,44503028,44500304,44503027,36543282,36521175,36533946,36538064,36522458,36550477,36559081,36517899,36526187,36542597,36544807,36529934,36554217,36529289,36538280,36565308,36542921,36544324,36562748,36562304,36533114,36526140,36520143,36519910,36519232,36566061,36526727,36554686,42512437,36524347,36523660,36527097,36536919,36527116,36520962,36554608,42511710,36537912,36538264,36537403,36561147,36534338,36567224,36523401,44502807,36520299,36528604,36542595,44500982,36531259,36561688,36532842,36551623,36558792,36557603,36525556,36565914,36534919,36537994,36554714,36520141,36545571,36562334,36560344,36551710,36527041,36565162,36525304,36522062,36520534,36521183,36540801,36553691,36517455,36524240,36548674,36547294,36545206,36543530,36521670,36517247,36546236,36531376,36545257,36546914,36557362,36525772,36523276,36563886,36555534,36533849,36562674,36563996,36527600,36557416,36552652,36529172,36562056,36546853,36534735,36539862,36524266,36526725,36540554,44500195,36540621,36518490,42512257,36528941,36565536,36554371,36555799,36535609,36548319,36532259,36533085,36528981,36562521,36549699,36547761,36563388,36562846,36566672,36562567,36524115,36553663,36533583,36534801,36520887,44502441,36552866,44500724,36537978,36560108,44501720,36521028,44501850,36568049,36568046,36568145,36568113,36568035,36568126,36568095,36551747,36547648,36566376,36526028,36539337,36527817,36563985,36520999,36534160,36521018,36530672,36555955,36555879,36552799,36518724,44499547,44499548,36549738,44503088,36518497,44500502)

) I
) C UNION ALL
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4207182,4116240,4193165,4207183,4115028,4193872,4193871,760959,36713361,36715911,37208245,36717495,36715912,4247719,432837,441800,438979,436635,437798,432257)

) I
) C UNION ALL
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1635345,1635187,1633469,1634637,1635160,1634424,1635303,1634921,1635536,1634892,1635336,1634268,1635008,1634188,1634325,1634362,1634606,1633916,1634891,1635097,1634712,1634657,1634526,1633757,1634618,1633457,1635505,1634312,1634093,1635338,1635373,1633421)

) I
) C UNION ALL
SELECT 3 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1635505)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1635505)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1635085)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1635085)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 5 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1634194,1634042,1634829,1633709,1634442,1635067,1634427,1634899,1633468,1635549,1633276,1635878,1635302,1635461,1633666,1635462,1634757,1635101,1633974,1635149,1633375,1633784,1633799,1634146,1635291,1635856,1635085,1633777,1635090,1635255,1634048,1634755)

) I
) C UNION ALL
SELECT 6 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1633702)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1633702)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 7 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1635566)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1635566)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 8 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4144721,3654354,4137462,4198190,35803548,35806902,4067713,35806140,4029571,40317890,4087760,35804767,35804755,35804560,35804247,35804227,35806400,35804156,35803572,35806172,35804537,4273629,4242997,35805329,35803573,4209537,40488297,4162987,4303236,4327938,40487126,4123410,35804434,3190561,35803471,4275431,4030165,35804162,35804134,4252245,4144920,4115551,4046732,911986,3663243,37108568,35804228,35803691,35805075,4266622,4200391,4079713,37204027,35623140,4238646,4157779,4152745,4064257,35623141,4070880,4123402,4017464,4058605,4061550,35804761,35804771,35806596,4203442,35804135,4042907,4061650,4127886,35804581,4201147,4078310,4186445,35804083,35803410,35804764,35804435,35803692,4319281,4027426,35806382,40481893,4074288,4140965,4114635,37110049,4030150,4312613,4070879,4151121,4000882,4140686,4025904,36684841,35804136,35806384,4300662,4306298,4221694,35805078,35804166,4141456,4181781,35804168,4235738,4029715,40491520,4161415,4059385,4182572,4178017,46272861,4226983,4067461,4120985,35622628,4225427,4030148,4125482,4215577,4301351,35805793,35805772,4141448,4096461,4172438,35806139,4234536,4097958,35804789,4136166,4029570,4028897,35804101,4337034,40493194,4069129,764407)

) I
) C UNION ALL
SELECT 9 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4058605,4029715,4161415,4059385,4182572,4178017,46272861,35622628,4215577,764407)

) I
) C UNION ALL
SELECT 10 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (35803410)

) I
) C UNION ALL
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (35804138,35803688,35806140,912132,35804795,35804434,35804164,911986,35805075,35804581,35804083,35804435,35806118,35803677,35804269,35804800,35803678,35804569,35803432,35803487,35805691,35806139,35804222,35804101)

) I
) C UNION ALL
SELECT 12 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (35806382,37110049,35806384,37110088)

) I
) C UNION ALL
SELECT 13 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (35803548,35804767,35804755,35804560,35804247,35806577,35804227,35806400,35804156,35803572,35806401,35806172,35804537,4273629,35805329,35804228,35803691,35804761,35804771,35806596,35804769,35803692,35805078,35804166,35805772)

) I
) C
;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
  -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.procedure_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.procedure_date as sort_date
from
(
  select po.*
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 8))
) C

WHERE (C.procedure_date >= DATEFROMPARTS(2006, 01, 01) and C.procedure_date <= DATEFROMPARTS(2021, 12, 31))
-- End Procedure Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM
(
  SELECT co.*
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-180,P.START_DATE) AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
SELECT 1 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM
(
  SELECT co.*
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-180,P.START_DATE) AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) > 0
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 1 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_1
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.measurement_date as sort_date
from
(
  select m.*
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 6))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
SELECT 1 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.measurement_date as sort_date
from
(
  select m.*
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 7))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) > 0
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 2 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_2
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.measurement_date as sort_date
from
(
  select m.*
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 3))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
SELECT 1 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.measurement_date as sort_date
from
(
  select m.*
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 4))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

UNION ALL
-- Begin Criteria Group
select 2 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.measurement_date as sort_date
from
(
  select m.*
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 2))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
SELECT 1 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.measurement_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.measurement_date as sort_date
from
(
  select m.*
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 5))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) > 0
) G
-- End Criteria Group

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) > 0
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 3 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_3
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.procedure_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.procedure_date as sort_date
from
(
  select po.*
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 9))
) C


-- End Procedure Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
SELECT 1 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from
(
  select de.*
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 10))
) C


-- End Drug Exposure Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
SELECT 2 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from
(
  select de.*
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 11))
) C


-- End Drug Exposure Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
SELECT 3 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from
(
  select de.*
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 12))
) C


-- End Drug Exposure Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 4
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 4 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_4
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from
(
  select de.*
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 13))
) C


-- End Drug Exposure Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) <= 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 5 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_5
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Death Criteria
select C.person_id, C.person_id as event_id, C.death_date as start_date, DATEADD(d,1,C.death_date) as end_date,
       coalesce(C.cause_concept_id,0) as TARGET_CONCEPT_ID, CAST(NULL as bigint) as visit_occurrence_id,
       C.death_date as sort_date
from
(
  select d.*
  FROM @cdm_database_schema.DEATH d

) C


-- End Death Criteria


) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,30,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) <= 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_1
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_2
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_3
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_4
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_5) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;

TRUNCATE TABLE #Inclusion_1;
DROP TABLE #Inclusion_1;

TRUNCATE TABLE #Inclusion_2;
DROP TABLE #Inclusion_2;

TRUNCATE TABLE #Inclusion_3;
DROP TABLE #Inclusion_3;

TRUNCATE TABLE #Inclusion_4;
DROP TABLE #Inclusion_4;

TRUNCATE TABLE #Inclusion_5;
DROP TABLE #Inclusion_5;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),6)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results

;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;





TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
