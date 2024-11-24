*	============================================================================
*	Prepared by:	Olivier Menaouer
*	Organization:	Statistics for Development Division (SDD), SPC
*	For:			Samoa
*	Survey:			Samoa 2022 Labour Force Survey
*	Object:			Anonymization of dataset
*	Start date:		06/11/2023
*	Final update:	17/11/2023
*	============================================================================

version 15
set more off
clear all

tempfile temp1 temp2 temp3 temp4 temp5

*	SETTING GLOBALS
global suser = c(username)

global	base	"C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Samoa"
global	in		"$base\SPC_WSM_2022_LFS_v01_M\Data\Distribute"
global	out		"$base\SPC_WSM_2022_LFS_v01_M_v01_A_PUF\Data\Distribute"

* Process for anonymisation:
	**	1. Remove direct identifiers (names, phone numbers, GPS coordinates, other) - already done;
	**	2. Anonymise the household ID, cluster number, district number, etc.;
	**	3. Identify "keys" in the dataset that may be used to identify individuals / households;
	**	4. Run the data set through the Statistical Disclosure Control Shiny GUI (http://www.ihsn.org/software/disclosure-control-toolbox);
	**	5. Anonymise the "keys" by top/bottom coding, aggregating, other... as well as all sensitive variables
	**	6. Run the dataset through the SDC micro app to establish if the risk has sufficiently reduced.
	
*	-------------------------------------------------------------------------------------------------------------------------------
**	1. Remove direct identifiers (names, phone numbers, GPS coordinates, other) - already done
*	-------------------------------------------------------------------------------------------------------------------------------
	** Generating a Cover dataset
use "$in\SPC_WSM_2022_LFS_Person_v01", clear

	collapse (max) rururb int_res int_status fweight (count) roster_id, by(cn strata district village hhnum)
	labvars strata hhnum rururb int_res int_status roster_id "strata" "household number" "area of residence" "result of interview" "status of interview" "household size"
	rename roster_id hhsize
	
	*	Variable name cleaning
	foreach v of varlist _all{ 													// replacing all var labs to lower case for uniformity
	local u : variable label `v'
	local l=lower("`u'")
	label var `v' "`l'"
	}
	
saveold "$in\SPC_WSM_2022_LFS_Cover_v01", replace version(12)

*	-------------------------------------------------------------------------------------------------------------------------------
**	2. Anonymise the HH_ID, PSU, etc.
*	-------------------------------------------------------------------------------------------------------------------------------
	** We need to keep the original person id to merge with other data files
	** We need to keep the sampling weight

gen x = runiform()																// generate randon number for all households
sort x																			// sort randomly
gen anon_id07 = _n																// generate household ID that is in no particular order
	lab var anon_id07 "anonymized household id"
drop x
	
**	Cleaning the new dataset
order anon_id07, first

** Only keep relevant variables
keep anon_id07 fweight cn strata district village hhnum rururb int_res int_status hhsize

** We now have all the anonymised variables that we need for the analysis
	** We keep this as a secure (not for public access) master merge file
save `temp1', replace
	
*	PERSON DATASET	*
use "$in\SPC_WSM_2022_LFS_Person_v01",clear
	merge m:1 cn strata district village hhnum using `temp1', keepusing(anon_id07)
	assert _merge==3
	drop _merge

	egen pers_id =concat(anon_id07 roster_id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id

order anon_id07, first
drop int_avai total_hrs
la drop J3A1A J3A2A J3B1A J3B2A J3C1A J3D1A J3F1A J3G1A J3H1A J3I1A J3J1A J3K1A J3L1A J3M1A 
la drop K2A
drop ilo_key ilo_time ilo_sex ilo_age ilo_age_5yrbands ilo_age_10yrbands ilo_age_aggregate ilo_age_ythadult ilo_edu_isced11 ilo_edu_aggregate ilo_edu_attendance ilo_edu_intappaggregate ilo_placeofbirth ilo_citizenship ilo_relationship_details ilo_relationship_aggregate ilo_mrts_aggregate ilo_dsb_details ilo_dsb_aggregate

labvars strata int_res int_status "strata" "result of interview" "status of interview"
drop cn district village hhnum
la def MARITAL_STATUS	 9 "other/don't know", modify
	la val marital_status MARITAL_STATUS
order roster_id person_id fweight, a(anon_id07)
la var d1a "person number answering this module"
la def E15	3 "5-9", modify
	la val e15 E15
	
*	Variable name cleaning
foreach v of varlist _all{ 														// replacing all var labs to lower case for uniformity
local u : variable label `v'
local l=lower("`u'")
label var `v' "`l'"
}

save "$base\SPC_WSM_2022_LFS_v01_M_v01_A_PUF\Data\Original\SPC_WSM_2022_LFS_Person_v01", replace

use `temp1', clear

save "$base\SPC_WSM_2022_LFS_v01_M_v01_A_PUF\Data\Original\SPC_WSM_2022_LFS_Cover_v01", replace

drop cn district village hhnum

*	-------------------------------------------------------------------------------------------------------------------------------
**	3. Identify "keys" in the dataset that may be used to identify individuals / households
*	-------------------------------------------------------------------------------------------------------------------------------
*	COVER DATASET
*		Scenario 1: strata hhsize

*	PERSON DATASET
*		Scenario 1: sex age e1cb e1bb f1c d3h e1y1 e4g1
*		Scenario 2: sex age e1cb e1bb f1c e1y1 e4g1 h1m
*		Scenario 3: sex age b1g_year1 c1hb e1cb e1bb
*		Scenario 4: sex age b1g_year1 e3p e4aa k1a2_year k1a2_month

*	-------------------------------------------------------------------------------------------------------------------------------
**	4. Run the pre-anonymization datasets through the Statistical Disclosure Control Shiny GUI
*	-------------------------------------------------------------------------------------------------------------------------------
*	COVER DATASET
** Scenario 1: Risk disclosure: 0.80% (21.93)

*	PERSON DATASET
** Scenario 1: Risk disclosure: 0.87% ()
** Scenario 2: Risk disclosure: 0.13% ()
** Scenario 3: Risk disclosure: 1.37% ()
** Scenario 4: Risk disclosure: 4.48% ()

*	-------------------------------------------------------------------------------------------------------------------------------
**	5. Anonymise the sensitive variables by top/bottom coding, aggregating, other;
*	-------------------------------------------------------------------------------------------------------------------------------
*	COVER DATASET	*
* 1 variable anonymized: hhsize

label data "Cover dataset"

recode hhsize (20/max=20)
		la def hhsizel	20 "20 people or more"
		la val hhsize hhsizel

sort anon_id07

saveold "$out\SPC_WSM_2022_LFS_Cover_v01_PUF", replace version(12)

*	PERSON DATASET	*
use "$base\SPC_WSM_2022_LFS_v01_M_v01_A_PUF\Data\Original\SPC_WSM_2022_LFS_Person_v01", clear

* 74 variables anonymized: 

label data "Person dataset"

drop b1g_month1 k1a2_month k1h_month k1l_month k1o_month k2c_month k2s_month k3f_month

recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
	la def ageyl	1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
					12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
	la val age_grp5 ageyl
		order age_grp5, after(age)
		la var age_grp5 "recode of age: age group 5"
	drop age year
recode k3b (6 7=6)
	la def k3bl	6 "6 or more"
	la val k3b k3bl
recode b1f1 (15/20=15)
recode b1g_year1 (1921/1979=1) (1980/1999=2) (2000/2014=3) (2015/2019=4) (2020/2022=5)
	la def b1g_year1l	1 "Before 1980" 2 "1980-1999" 3 "2000-2014" 4 "2015-2019" 5 "2020-2022" 9998 "Don't know" 9999 "Refuse"
	la val b1g_year1 b1g_year1l
recode c1f (3/7=3)
	la def C1F	3 "2 weeks or more", modify
	la val c1f C1F
recode c1hb (11 113 221 311 312 413 421 431 521 522 532 611 712 713 732 831 921 922 1011 1021 10232=9999) 
	la def C1HB	9999 "Other", modify
	la val c1hb C1HB
recode c1j (2 3 4 5=6)

***	ISIC - 2 digits
	gen d3ea_2digits = . 
	    replace d3ea_2digits = int(d3ea/100)
			    lab def isic2_lab		1 "01 - Crop and animal production, hunting and related service activities"	2 "02 - Forestry and logging"	3 "03 - Fishing and aquaculture"	5 "05 - Mining of coal and lignite" ///
                                        6 "06 - Extraction of crude petroleum and natural gas"	7 "07 - Mining of metal ores"	8 "08 - Other mining and quarrying"	9 "09 - Mining support service activities" ///
                                        10 "10 - Manufacture of food products"	11 "11 - Manufacture of beverages"	12 "12 - Manufacture of tobacco products"	13 "13 - Manufacture of textiles" ///
                                        14 "14 - Manufacture of wearing apparel"	15 "15 - Manufacture of leather and related products"	16 "16 - Manufacture of wood and of products of wood and cork, except furniture; manufacture of articles of straw and plaiting materials"	17 "17 - Manufacture of paper and paper products" ///
                                        18 "18 - Printing and reproduction of recorded media"	19 "19 - Manufacture of coke and refined petroleum products"	20 "20 - Manufacture of chemicals and chemical products"	21 "21 - Manufacture of pharmaceuticals, medicinal chemical and botanical products" ///
                                        22 "22 - Manufacture of rubber and plastics products"	23 "23 - Manufacture of other non-metallic mineral products"	24 "24 - Manufacture of basic metals"	25 "25 - Manufacture of fabricated metal products, except machinery and equipment" ///
                                        26 "26 - Manufacture of computer, electronic and optical products"	27 "27 - Manufacture of electrical equipment"	28 "28 - Manufacture of machinery and equipment n.e.c."	29 "29 - Manufacture of motor vehicles, trailers and semi-trailers" ///
                                        30 "30 - Manufacture of other transport equipment"	31 "31 - Manufacture of furniture"	32 "32 - Other manufacturing"	33 "33 - Repair and installation of machinery and equipment" ///
                                        35 "35 - Electricity, gas, steam and air conditioning supply"	36 "36 - Water collection, treatment and supply"	37 "37 - Sewerage"	38 "38 - Waste collection, treatment and disposal activities; materials recovery" ///
                                        39 "39 - Remediation activities and other waste management services"	41 "41 - Construction of buildings"	42 "42 - Civil engineering"	43 "43 - Specialized construction activities" ///
                                        45 "45 - Wholesale and retail trade and repair of motor vehicles and motorcycles"	46 "46 - Wholesale trade, except of motor vehicles and motorcycles"	47 "47 - Retail trade, except of motor vehicles and motorcycles"	49 "49 - Land transport and transport via pipelines" ///
                                        50 "50 - Water transport"	51 "51 - Air transport"	52 "52 - Warehousing and support activities for transportation"	53 "53 - Postal and courier activities" ///
                                        55 "55 - Accommodation"	56 "56 - Food and beverage service activities"	58 "58 - Publishing activities"	59 "59 - Motion picture, video and television programme production, sound recording and music publishing activities" ///
                                        60 "60 - Programming and broadcasting activities"	61 "61 - Telecommunications"	62 "62 - Computer programming, consultancy and related activities"	63 "63 - Information service activities" ///
                                        64 "64 - Financial service activities, except insurance and pension funding"	65 "65 - Insurance, reinsurance and pension funding, except compulsory social security"	66 "66 - Activities auxiliary to financial service and insurance activities"	68 "68 - Real estate activities" ///
                                        69 "69 - Legal and accounting activities"	70 "70 - Activities of head offices; management consultancy activities"	71 "71 - Architectural and engineering activities; technical testing and analysis"	72 "72 - Scientific research and development" ///
                                        73 "73 - Advertising and market research"	74 "74 - Other professional, scientific and technical activities"	75 "75 - Veterinary activities"	77 "77 - Rental and leasing activities" ///
                                        78 "78 - Employment activities"	79 "79 - Travel agency, tour operator, reservation service and related activities"	80 "80 - Security and investigation activities"	81 "81 - Services to buildings and landscape activities" ///
                                        82 "82 - Office administrative, office support and other business support activities"	84 "84 - Public administration and defence; compulsory social security"	85 "85 - Education"	86 "86 - Human health activities" ///
                                        87 "87 - Residential care activities"	88 "88 - Social work activities without accommodation"	90 "90 - Creative, arts and entertainment activities"	91 "91 - Libraries, archives, museums and other cultural activities" ///
                                        92 "92 - Gambling and betting activities"	93 "93 - Sports activities and amusement and recreation activities"	94 "94 - Activities of membership organizations"	95 "95 - Repair of computers and personal and household goods" ///
                                        96 "96 - Other personal service activities"	97 "97 - Activities of households as employers of domestic personnel"	98 "98 - Undifferentiated goods- and services-producing activities of private households for own use"	99 "99 - Activities of extraterritorial organizations and bodies"
                lab val d3ea_2digits isic2_lab

***	ISIC - 1 digit
    gen d3ea_1digit=.
	    replace d3ea_1digit=1 if inrange(d3ea_2digits,1,3)
	    replace d3ea_1digit=2 if inrange(d3ea_2digits,5,9)
	    replace d3ea_1digit=3 if inrange(d3ea_2digits,10,33)
	    replace d3ea_1digit=4 if d3ea_2digits==35
	    replace d3ea_1digit=5 if inrange(d3ea_2digits,36,39)
	    replace d3ea_1digit=6 if inrange(d3ea_2digits,41,43)
	    replace d3ea_1digit=7 if inrange(d3ea_2digits,45,47)
	    replace d3ea_1digit=8 if inrange(d3ea_2digits,49,53)
	    replace d3ea_1digit=9 if inrange(d3ea_2digits,55,56)
	    replace d3ea_1digit=10 if inrange(d3ea_2digits,58,63)
	    replace d3ea_1digit=11 if inrange(d3ea_2digits,64,66)
	    replace d3ea_1digit=12 if d3ea_2digits==68
	    replace d3ea_1digit=13 if inrange(d3ea_2digits,69,75)		
	    replace d3ea_1digit=14 if inrange(d3ea_2digits,77,82)
	    replace d3ea_1digit=15 if d3ea_2digits==84
        replace d3ea_1digit=16 if d3ea_2digits==85
	    replace d3ea_1digit=17 if inrange(d3ea_2digits,86,88)
	    replace d3ea_1digit=18 if inrange(d3ea_2digits,90,93)
	    replace d3ea_1digit=19 if inrange(d3ea_2digits,94,96)
	    replace d3ea_1digit=20 if inrange(d3ea_2digits,97,98)
	    replace d3ea_1digit=21 if d3ea_2digits==99
	    replace d3ea_1digit=22 if d3ea_2digits==.
		        lab def isic1_lab 		1 "A - Agriculture, forestry and fishing"	2 "B - Mining and quarrying"	3 "C - Manufacturing"	4 "D - Electricity, gas, steam and air conditioning supply" ///
                                        5 "E - Water supply; sewerage, waste management and remediation activities"	6 "F - Construction"	7 "G - Wholesale and retail trade; repair of motor vehicles and motorcycles"	8 "H - Transportation and storage" ///
                                        9 "I - Accommodation and food service activities"	10 "J - Information and communication"	11 "K - Financial and insurance activities"	12 "L - Real estate activities" ///
                                        13 "M - Professional, scientific and technical activities"	14 "N - Administrative and support service activities"	15 "O - Public administration and defence; compulsory social security"	16 "P - Education" ///
                                        17 "Q - Human health and social work activities"	18 "R - Arts, entertainment and recreation"	19 "S - Other service activities"	20 "T - Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use" ///
                                        21 "U - Activities of extraterritorial organizations and bodies"	22 "X - Not elsewhere classified"		
  	  		    lab val d3ea_1digit isic1_lab
				la var d3ea_1digit "d3ea - ISIC Rev 4 - Major (1 digit)"
				la var d3ea_2digits "d3ea - ISIC Rev 4 - Sub-major (2 digits)"
				order d3ea_2digits d3ea_1digit, a(d3ea)
	drop d3ea

 recode d3h (42/max=42)
	la def  d3hl	42 "42 hours or more"
	la val d3h d3hl

***	ISCO - 2 digits
    gen e1bb_2digits = . 
	    replace e1bb_2digits = int( e1bb/100)
		        lab def isco2_lab 		1 "01 - Commissioned armed forces officers"	2 "02 - Non-commissioned armed forces officers"	3 "03 - Armed forces occupations, other ranks"	11 "11 - Chief executives, senior officials and legislators"	///
                                        12 "12 - Administrative and commercial managers"	13 "13 - Production and specialised services managers"	14 "14 - Hospitality, retail and other services managers"	21 "21 - Science and engineering professionals"	///
                                        22 "22 - Health professionals"	23 "23 - Teaching professionals"	24 "24 - Business and administration professionals"	25 "25 - Information and communications technology professionals"	///
                                        26 "26 - Legal, social and cultural professionals"	31 "31 - Science and engineering associate professionals"	32 "32 - Health associate professionals"	33 "33 - Business and administration associate professionals"	///
                                        34 "34 - Legal, social, cultural and related associate professionals"	35 "35 - Information and communications technicians"	41 "41 - General and keyboard clerks"	42 "42 - Customer services clerks"	///
                                        43 "43 - Numerical and material recording clerks"	44 "44 - Other clerical support workers"	51 "51 - Personal service workers"	52 "52 - Sales workers"	///
                                        53 "53 - Personal care workers"	54 "54 - Protective services workers"	61 "61 - Market-oriented skilled agricultural workers"	62 "62 - Market-oriented skilled forestry, fishery and hunting workers"	///
                                        63 "63 - Subsistence farmers, fishers, hunters and gatherers"	71 "71 - Building and related trades workers, excluding electricians"	72 "72 - Metal, machinery and related trades workers"	73 "73 - Handicraft and printing workers"	///
                                        74 "74 - Electrical and electronic trades workers"	75 "75 - Food processing, wood working, garment and other craft and related trades workers"	81 "81 - Stationary plant and machine operators"	82 "82 - Assemblers"	///
                                        83 "83 - Drivers and mobile plant operators"	91 "91 - Cleaners and helpers"	92 "92 - Agricultural, forestry and fishery labourers"	93 "93 - Labourers in mining, construction, manufacturing and transport"	///
                                        94 "94 - Food preparation assistants"	95 "95 - Street and related sales and service workers"	96 "96 - Refuse workers and other elementary workers"		
	            lab values e1bb_2digits isco2_lab

***	ISCO - 1 digit
	gen e1bb_1digit=.
	    replace e1bb_1digit=11 if inlist(e1bb_2digits,.)								// Not elsewhere classified
		replace e1bb_1digit=int(e1bb_2digits/10) if e1bb_1digit==.						// The rest of the occupations
		replace e1bb_1digit=10 if e1bb_1digit==0										// Armed forces
		        lab def isco1_lab 		1 "1 - Managers"	2 "2 - Professionals"	3 "3 - Technicians and associate professionals"	4 "4 - Clerical support workers"	///
                                        5 "5 - Service and sales workers"	6 "6 - Skilled agricultural, forestry and fishery workers"	7 "7 - Craft and related trades workers"	8 "8 - Plant and machine operators, and assemblers"	///
                                        9 "9 - Elementary occupations"	10 "0 - Armed forces occupations"	11 "X - Not elsewhere classified"		
				lab val e1bb_1digit isco1_lab

la var e1bb_1digit "e1bb - ISCO 08 - Major (1 digit)"
la var e1bb_2digits "e1bb - ISCO 08 - Sub-major (2 digits)"
	order e1bb_2digits e1bb_1digit, a(e1bb)

***	ISIC - 2 digits
	gen e1cb_2digits = . 
	    replace e1cb_2digits = int(e1cb/100)
                lab val e1cb_2digits isic2_lab

***	ISIC - 1 digit
    gen e1cb_1digit=.
	    replace e1cb_1digit=1 if inrange(e1cb_2digits,1,3)
	    replace e1cb_1digit=2 if inrange(e1cb_2digits,5,9)
	    replace e1cb_1digit=3 if inrange(e1cb_2digits,10,33)
	    replace e1cb_1digit=4 if e1cb_2digits==35
	    replace e1cb_1digit=5 if inrange(e1cb_2digits,36,39)
	    replace e1cb_1digit=6 if inrange(e1cb_2digits,41,43)
	    replace e1cb_1digit=7 if inrange(e1cb_2digits,45,47)
	    replace e1cb_1digit=8 if inrange(e1cb_2digits,49,53)
	    replace e1cb_1digit=9 if inrange(e1cb_2digits,55,56)
	    replace e1cb_1digit=10 if inrange(e1cb_2digits,58,63)
	    replace e1cb_1digit=11 if inrange(e1cb_2digits,64,66)
	    replace e1cb_1digit=12 if e1cb_2digits==68
	    replace e1cb_1digit=13 if inrange(e1cb_2digits,69,75)		
	    replace e1cb_1digit=14 if inrange(e1cb_2digits,77,82)
	    replace e1cb_1digit=15 if e1cb_2digits==84
        replace e1cb_1digit=16 if e1cb_2digits==85
	    replace e1cb_1digit=17 if inrange(e1cb_2digits,86,88)
	    replace e1cb_1digit=18 if inrange(e1cb_2digits,90,93)
	    replace e1cb_1digit=19 if inrange(e1cb_2digits,94,96)
	    replace e1cb_1digit=20 if inrange(e1cb_2digits,97,98)
	    replace e1cb_1digit=21 if e1cb_2digits==99
	    replace e1cb_1digit=22 if e1cb_2digits==.
  	  		    lab val e1cb_1digit isic1_lab
				la var e1cb_1digit "e1cb - ISIC Rev 4 - Major (1 digit)"
				la var e1cb_2digits "e1cb - ISIC Rev 4 - Sub-major (2 digits)"
					order e1cb_2digits e1cb_1digit, a(e1cb)
	drop e1bb e1cb
	
recode e1l (60/max=60)
	la def e1ll	63 "60 hours or more"
	la val e1l e1ll
recode e1v (1964/2009=1)
	la def e1vl	1 "before 2010"
	la val e1v e1vl
recode e1y1 (6001/9000=6001)
	la def e1y1l	6001 "more than 6000" 9998 "Don't know"
	la val e1y1 e1y1l
recode e1y2 (500/max=500)
	la def	e1y2l	500 "500 or more"
	la val e1y2 e1y2l
recode e3cm (12/max=12)
	la def e3cml	12 "12 or more"
	la val e3cm e3cml
recode e3cv (6/max=6)
	la def e3cvl	6 "6 or more"
	la val e3cv e3cvl
recode e3g (60/max=60)
	la def e3gl	60 "60 hours or more"
	la val e3g e3gl
recode e3hm (6/max=6)
	la def e3hml	6 "6 or more"
	la val e3hm e3hml
recode e3hv (6/max=6)
	la def e3hvl	6 "6 or more"
	la val e3hv e3hvl	
recode e3km (6/max=6)
	la def e3kml	6 "6 or more"
	la val e3km e3kml
recode e3kv (5/max=5)
	la def e3kvl	5 "5 or more"
	la val e3kv e3kvl
recode e3p (4001/8000=4001) (8888 9998=8888)
	la def e3pl	4001 "more than 4000" 8888 "don't know" 9999 "refused"
	la val e3p e3pl
recode e3q (7/24=7)
	la def e3ql	7 "7 or more" 88 "don't know" 99 "refused"
	la val e3q e3ql

***	ISCO - 2 digits
    gen e4aa_2digits = . 
	    replace e4aa_2digits = int( e4aa/100)
	            lab values e4aa_2digits isco2_lab

***	ISCO - 1 digit
	gen e4aa_1digit=.
	    replace e4aa_1digit=11 if inlist(e4aa_2digits,.)									// Not elsewhere classified
		replace e4aa_1digit=int(e4aa_2digits/10) if e4aa_1digit==.							// The rest of the occupations
		replace e4aa_1digit=10 if e4aa_1digit==0											// Armed forces
				lab val e4aa_1digit isco1_lab

la var e4aa_1digit "e4aa - ISCO 08 - Major (1 digit)"
la var e4aa_2digits "e4aa - ISCO 08 - Sub-major (2 digits)"
	order e4aa_2digits e4aa_1digit, a(e4aa)

***	ISIC - 2 digits
	gen e4bb_2digits = . 
	    replace e4bb_2digits = int(e4bb/100)
                lab val e4bb_2digits isic2_lab

***	ISIC - 1 digit
    gen e4bb_1digit=.
	    replace e4bb_1digit=1 if inrange(e4bb_2digits,1,3)
	    replace e4bb_1digit=2 if inrange(e4bb_2digits,5,9)
	    replace e4bb_1digit=3 if inrange(e4bb_2digits,10,33)
	    replace e4bb_1digit=4 if e4bb_2digits==35
	    replace e4bb_1digit=5 if inrange(e4bb_2digits,36,39)
	    replace e4bb_1digit=6 if inrange(e4bb_2digits,41,43)
	    replace e4bb_1digit=7 if inrange(e4bb_2digits,45,47)
	    replace e4bb_1digit=8 if inrange(e4bb_2digits,49,53)
	    replace e4bb_1digit=9 if inrange(e4bb_2digits,55,56)
	    replace e4bb_1digit=10 if inrange(e4bb_2digits,58,63)
	    replace e4bb_1digit=11 if inrange(e4bb_2digits,64,66)
	    replace e4bb_1digit=12 if e4bb_2digits==68
	    replace e4bb_1digit=13 if inrange(e4bb_2digits,69,75)		
	    replace e4bb_1digit=14 if inrange(e4bb_2digits,77,82)
	    replace e4bb_1digit=15 if e4bb_2digits==84
        replace e4bb_1digit=16 if e4bb_2digits==85
	    replace e4bb_1digit=17 if inrange(e4bb_2digits,86,88)
	    replace e4bb_1digit=18 if inrange(e4bb_2digits,90,93)
	    replace e4bb_1digit=19 if inrange(e4bb_2digits,94,96)
	    replace e4bb_1digit=20 if inrange(e4bb_2digits,97,98)
	    replace e4bb_1digit=21 if e4bb_2digits==99
	    replace e4bb_1digit=22 if e4bb_2digits==.
  	  		    lab val e4bb_1digit isic1_lab
				la var e4bb_1digit "e4bb - ISIC Rev 4 - Major (1 digit)"
				la var e4bb_2digits "e4bb - ISIC Rev 4 - Sub-major (2 digits)"
					order e4bb_2digits e4bb_1digit, a(e4bb)
	drop e4aa e4bb

recode e4g1 (1300/7000=1300)
	la def e4g1l	1300 "1300 or more"
	la val e4g1 e4g1l

recode f1c (90/98=90)
	la def f1cl	90 "90 or more"
	la val f1c f1cl
recode f1f (40/85=40) 
	la def f1fl	40 "40 or more" 98 "don't know"
	la val f1f f1fl
recode f1g (90/97=90)
	la def f1gl	90 "90 or more" 98 "don't know"
	la val f1g f1gl
recode f1h (30/97=30)
	la def f1hl	30 "30 or more" 98 "don't know"
	la val f1h f1hl
recode f1j (30/50=30)
	la def f1jl	30 "30 or more" 98 "don't know"
	la val f1j f1jl
recode f1o (15/97=15)
	la def f1ol	15 "15 or more" 98 "don't know"
	la val f1o f1ol

***	ISCO - 2 digits
    gen h1ea_2digits = . 
	    replace h1ea_2digits = int( h1ea/100)
	            lab values h1ea_2digits isco2_lab

***	ISCO - 1 digit
	gen h1ea_1digit=.
	    replace h1ea_1digit=11 if inlist(h1ea_2digits,.)									// Not elsewhere classified
		replace h1ea_1digit=int(h1ea_2digits/10) if e4aa_1digit==.							// The rest of the occupations
		replace h1ea_1digit=10 if h1ea_1digit==0											// Armed forces
				lab val h1ea_1digit isco1_lab

la var h1ea_1digit "h1ea - ISCO 08 - Major (1 digit)"
la var h1ea_2digits "h1ea - ISCO 08 - Sub-major (2 digits)"
	order h1ea_2digits h1ea_1digit, a(h1ea)

***	ISIC - 2 digits
	gen h1fa_2digits = . 
	    replace h1fa_2digits = int(h1fa/100)
                lab val h1fa_2digits isic2_lab

***	ISIC - 1 digit
    gen h1fa_1digit=.
	    replace h1fa_1digit=1 if inrange(h1fa_2digits,1,3)
	    replace h1fa_1digit=2 if inrange(h1fa_2digits,5,9)
	    replace h1fa_1digit=3 if inrange(h1fa_2digits,10,33)
	    replace h1fa_1digit=4 if h1fa_2digits==35
	    replace h1fa_1digit=5 if inrange(h1fa_2digits,36,39)
	    replace h1fa_1digit=6 if inrange(h1fa_2digits,41,43)
	    replace h1fa_1digit=7 if inrange(h1fa_2digits,45,47)
	    replace h1fa_1digit=8 if inrange(h1fa_2digits,49,53)
	    replace h1fa_1digit=9 if inrange(h1fa_2digits,55,56)
	    replace h1fa_1digit=10 if inrange(h1fa_2digits,58,63)
	    replace h1fa_1digit=11 if inrange(h1fa_2digits,64,66)
	    replace h1fa_1digit=12 if h1fa_2digits==68
	    replace h1fa_1digit=13 if inrange(h1fa_2digits,69,75)		
	    replace h1fa_1digit=14 if inrange(h1fa_2digits,77,82)
	    replace h1fa_1digit=15 if h1fa_2digits==84
        replace h1fa_1digit=16 if h1fa_2digits==85
	    replace h1fa_1digit=17 if inrange(h1fa_2digits,86,88)
	    replace h1fa_1digit=18 if inrange(h1fa_2digits,90,93)
	    replace h1fa_1digit=19 if inrange(h1fa_2digits,94,96)
	    replace h1fa_1digit=20 if inrange(h1fa_2digits,97,98)
	    replace h1fa_1digit=21 if h1fa_2digits==99
	    replace h1fa_1digit=22 if h1fa_2digits==.
  	  		    lab val h1fa_1digit isic1_lab
				la var h1fa_1digit "h1fa - ISIC Rev 4 - Major (1 digit)"
				la var h1fa_2digits "h1fa - ISIC Rev 4 - Sub-major (2 digits)"
					order h1fa_2digits h1fa_1digit, a(h1fa)
	drop h1ea h1fa
	
recode h1m (8888=88888) (9999=99999) (4000/120000=4000)
	la def h1ml	88888 "don't know" 99999 "refused" 4000 "4000 or more"
	la val h1m h1ml

***	ISCO - 2 digits
    gen h2db_2digits = . 
	    replace h2db_2digits = int( h2db/100)
	            lab values h2db_2digits isco2_lab

***	ISCO - 1 digit
	gen h2db_1digit=.
	    replace h2db_1digit=11 if inlist(h2db_2digits,.)									// Not elsewhere classified
		replace h2db_1digit=int(h2db_2digits/10) if h2db_1digit==.							// The rest of the occupations
		replace h2db_1digit=10 if h2db_1digit==0											// Armed forces
				lab val h2db_1digit isco1_lab

la var h2db_1digit "h2db - ISCO 08 - Major (1 digit)"
la var h2db_2digits "h2db - ISCO 08 - Sub-major (2 digits)"
	order h2db_2digits h2db_1digit, a(h2db)

***	ISIC - 2 digits
	gen h2fb_2digits = . 
	    replace h2fb_2digits = int(h2fb/100)
                lab val h2fb_2digits isic2_lab

***	ISIC - 1 digit
    gen h2fb_1digit=.
	    replace h2fb_1digit=1 if inrange(h2fb_2digits,1,3)
	    replace h2fb_1digit=2 if inrange(h2fb_2digits,5,9)
	    replace h2fb_1digit=3 if inrange(h2fb_2digits,10,33)
	    replace h2fb_1digit=4 if h2fb_2digits==35
	    replace h2fb_1digit=5 if inrange(h2fb_2digits,36,39)
	    replace h2fb_1digit=6 if inrange(h2fb_2digits,41,43)
	    replace h2fb_1digit=7 if inrange(h2fb_2digits,45,47)
	    replace h2fb_1digit=8 if inrange(h2fb_2digits,49,53)
	    replace h2fb_1digit=9 if inrange(h2fb_2digits,55,56)
	    replace h2fb_1digit=10 if inrange(h2fb_2digits,58,63)
	    replace h2fb_1digit=11 if inrange(h2fb_2digits,64,66)
	    replace h2fb_1digit=12 if h2fb_2digits==68
	    replace h2fb_1digit=13 if inrange(h2fb_2digits,69,75)		
	    replace h2fb_1digit=14 if inrange(h2fb_2digits,77,82)
	    replace h2fb_1digit=15 if h2fb_2digits==84
        replace h2fb_1digit=16 if h2fb_2digits==85
	    replace h2fb_1digit=17 if inrange(h2fb_2digits,86,88)
	    replace h2fb_1digit=18 if inrange(h2fb_2digits,90,93)
	    replace h2fb_1digit=19 if inrange(h2fb_2digits,94,96)
	    replace h2fb_1digit=20 if inrange(h2fb_2digits,97,98)
	    replace h2fb_1digit=21 if h2fb_2digits==99
	    replace h2fb_1digit=22 if h2fb_2digits==.
  	  		    lab val h2fb_1digit isic1_lab
				la var h2fb_1digit "h2fb - ISIC Rev 4 - Major (1 digit)"
				la var h2fb_2digits "h2fb - ISIC Rev 4 - Sub-major (2 digits)"
					order h2fb_2digits h2fb_1digit, a(h2fb)
	drop h2db h2fb

***	ISIC - 2 digits
	gen j1bb_2digits = . 
	    replace j1bb_2digits = int(j1bb/100)
                lab val j1bb_2digits isic2_lab

***	ISIC - 1 digit
    gen j1bb_1digit=.
	    replace j1bb_1digit=1 if inrange(j1bb_2digits,1,3)
	    replace j1bb_1digit=2 if inrange(j1bb_2digits,5,9)
	    replace j1bb_1digit=3 if inrange(j1bb_2digits,10,33)
	    replace j1bb_1digit=4 if j1bb_2digits==35
	    replace j1bb_1digit=5 if inrange(j1bb_2digits,36,39)
	    replace j1bb_1digit=6 if inrange(j1bb_2digits,41,43)
	    replace j1bb_1digit=7 if inrange(j1bb_2digits,45,47)
	    replace j1bb_1digit=8 if inrange(j1bb_2digits,49,53)
	    replace j1bb_1digit=9 if inrange(j1bb_2digits,55,56)
	    replace j1bb_1digit=10 if inrange(j1bb_2digits,58,63)
	    replace j1bb_1digit=11 if inrange(j1bb_2digits,64,66)
	    replace j1bb_1digit=12 if j1bb_2digits==68
	    replace j1bb_1digit=13 if inrange(j1bb_2digits,69,75)		
	    replace j1bb_1digit=14 if inrange(j1bb_2digits,77,82)
	    replace j1bb_1digit=15 if j1bb_2digits==84
        replace j1bb_1digit=16 if j1bb_2digits==85
	    replace j1bb_1digit=17 if inrange(j1bb_2digits,86,88)
	    replace j1bb_1digit=18 if inrange(j1bb_2digits,90,93)
	    replace j1bb_1digit=19 if inrange(j1bb_2digits,94,96)
	    replace j1bb_1digit=20 if inrange(j1bb_2digits,97,98)
	    replace j1bb_1digit=21 if j1bb_2digits==99
	    replace j1bb_1digit=22 if j1bb_2digits==.
  	  		    lab val j1bb_1digit isic1_lab
				la var j1bb_1digit "j1bb - ISIC Rev 4 - Major (1 digit)"
				la var j1bb_2digits "j1bb - ISIC Rev 4 - Sub-major (2 digits)"
					order j1bb_2digits j1bb_1digit, a(j1bb)
	drop j1bb

recode j1b2 (15/20=15)
	la def j1b2l	15 "15 or more"
	la val j1b2 j1b2l
recode j1c1 (10/14=10)
	la def j1c1l	10 "10 or more"
	la val j1c1 j1c1l
recode j1d1 (20/35=20)
	la def j1d1l	20 "20 or more"
	la val j1d1 j1d1l
recode j1e1 (15/40=15)
	la def j1e1l	15 "15 or more"
	la val j1e1 j1e1l
recode j1f1 (20/40=20)
	la def j1f1l	20 "20 or more"
	la val j1f1 j1f1l
recode j1h1 (14/28=14)
	la def j1h1l	14 "14 or more"
	la val j1h1 j1h1l
recode j2c1 (50/80=50)
	la def j2c1l	50 "50 or more"
	la val j2c1 j2c1l
recode j3n (27/38=27)
	la def j3nl	27 "27 or more"
	la val j3n j3nl
recode k1a (5 6=8)
recode k1a1 (5 6=8)
recode k1a2_year (1958/1989=1989) (9008 999=9998)
	la def k1a2_yearl	1989 "before 1990" 9998 "don't know"
	la val k1a2_year k1a2_yearl
recode k1c (4 5=7)
recode k1h_year (1956/1989=1989) (2919=2019)
	la def k1h_yearl	1989 "before 1989" 9998 "don't know"
	la val k1h_year k1h_yearl
recode k1l_year (1967/1989=1989)
	la def k1l_yearl	1989 "before 1989" 9998 "don't know"
	la val k1l_year k1l_yearl
recode k1o_year (1976/1999=1999)
	la def k1o_yearl	1999 "before 1999" 9998 "don't know"
	la val k1o_year k1o_yearl
recode k2c_year (1969/1999=1999)
	la def k2c_yearl	1999 "before 1999" 9998 "don't know"
	la val k2c_year k2c_yearl

***	ISCO - 2 digits
    gen k2e1_2digits = . 
	    replace k2e1_2digits = int( k2e1/100)
	            lab values k2e1_2digits isco2_lab

***	ISCO - 1 digit
	gen k2e1_1digit=.
	    replace k2e1_1digit=11 if inlist(k2e1_2digits,.)									// Not elsewhere classified
		replace k2e1_1digit=int(k2e1_2digits/10) if k2e1_1digit==.							// The rest of the occupations
		replace k2e1_1digit=10 if k2e1_1digit==0											// Armed forces
				lab val k2e1_1digit isco1_lab

la var k2e1_1digit "k2e1 - ISCO 08 - Major (1 digit)"
la var k2e1_2digits "k2e1 - ISCO 08 - Sub-major (2 digits)"
	order k2e1_2digits k2e1_1digit, a(k2e1)
drop k2e1

***	ISIC - 2 digits
	gen k2f1_2digits = . 
	    replace k2f1_2digits = int(k2f1/100)
                lab val k2f1_2digits isic2_lab

***	ISIC - 1 digit
    gen k2f1_1digit=.
	    replace k2f1_1digit=1 if inrange(k2f1_2digits,1,3)
	    replace k2f1_1digit=2 if inrange(k2f1_2digits,5,9)
	    replace k2f1_1digit=3 if inrange(k2f1_2digits,10,33)
	    replace k2f1_1digit=4 if k2f1_2digits==35
	    replace k2f1_1digit=5 if inrange(k2f1_2digits,36,39)
	    replace k2f1_1digit=6 if inrange(k2f1_2digits,41,43)
	    replace k2f1_1digit=7 if inrange(k2f1_2digits,45,47)
	    replace k2f1_1digit=8 if inrange(k2f1_2digits,49,53)
	    replace k2f1_1digit=9 if inrange(k2f1_2digits,55,56)
	    replace k2f1_1digit=10 if inrange(k2f1_2digits,58,63)
	    replace k2f1_1digit=11 if inrange(k2f1_2digits,64,66)
	    replace k2f1_1digit=12 if k2f1_2digits==68
	    replace k2f1_1digit=13 if inrange(k2f1_2digits,69,75)		
	    replace k2f1_1digit=14 if inrange(k2f1_2digits,77,82)
	    replace k2f1_1digit=15 if k2f1_2digits==84
        replace k2f1_1digit=16 if k2f1_2digits==85
	    replace k2f1_1digit=17 if inrange(k2f1_2digits,86,88)
	    replace k2f1_1digit=18 if inrange(k2f1_2digits,90,93)
	    replace k2f1_1digit=19 if inrange(k2f1_2digits,94,96)
	    replace k2f1_1digit=20 if inrange(k2f1_2digits,97,98)
	    replace k2f1_1digit=21 if k2f1_2digits==99
	    replace k2f1_1digit=22 if k2f1_2digits==.
  	  		    lab val k2f1_1digit isic1_lab
				la var k2f1_1digit "k2f1 - ISIC Rev 4 - Major (1 digit)"
				la var k2f1_2digits "k2f1 - ISIC Rev 4 - Sub-major (2 digits)"
					order k2f1_2digits k2f1_1digit, a(k2f1)
drop k2f1

recode k3e (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(k3e_grp5)
	la val k3e_grp5 ageyl
		order k3e_grp5, after(k3e)
		la var k3e_grp5 "recode of k3e: age group 5"
	drop k3e
recode k3f_year (1995/2017=2017)
	la def k3f_yearl	2017 "before 2018" 9998 "don't know"
	la val k3f_year k3f_yearl

***	ISCO - 2 digits
    gen k3ja_2digits = . 
	    replace k3ja_2digits = int(k3ja/100)
	            lab values k3ja_2digits isco2_lab

***	ISCO - 1 digit
	gen k3ja_1digit=.
	    replace k3ja_1digit=11 if inlist(k3ja_2digits,.)									// Not elsewhere classified
		replace k3ja_1digit=int(k3ja_2digits/10) if k3ja_1digit==.							// The rest of the occupations
		replace k3ja_1digit=10 if k3ja_1digit==0											// Armed forces
				lab val k3ja_1digit isco1_lab

la var k3ja_1digit "k3ja - ISCO 08 - Major (1 digit)"
la var k3ja_2digits "k3ja - ISCO 08 - Sub-major (2 digits)"
	order k3ja_2digits k3ja_1digit, a(k3ja)
drop k3ja
drop ilo_job1_eco_isic4_4digits ilo_job1_ocu_isco08_4digits ilo_job1_ocu_isco08_3digits ilo_job1_eco_isic4_3digits ilo_job1_eco_isic4_2digits ilo_job2_eco_isic4_2digits ilo_job1_ocu_isco08_2digits ilo_job2_ocu_isco08_2digits ilo_prevocu_isco08_2digits
recode ilo_job1_how_usual (90/96=90)
	la def ilo_job1_how_usuall	90 "90 or more" 98 "don't know"
	la val ilo_job1_how_usual ilo_job1_how_usuall
recode ilo_job1_how_actual (90/96=90)
	la def ilo_job1_how_actuall	90 "90 or more" 98 "don't know"
	la val ilo_job1_how_actual ilo_job1_how_actuall
recode ilo_job2_how_usual (36/70=36)
	la def ilo_job2_how_usuall	36 "36 or more" 98 "don't know"
	la val ilo_job2_how_usual ilo_job2_how_usuall
recode ilo_job2_how_actual (36/50=36)
	la def ilo_job2_how_actuall	36 "36 or more" 98 "don't know"
	la val ilo_job2_how_actual ilo_job2_how_actuall
recode ilo_joball_how_usual (91/140=91) (98=998)
	la def ilo_joball_how_usuall	91 "91 or more" 998 "don't know"
	la val ilo_joball_how_usual ilo_joball_how_usuall
recode ilo_joball_how_actual (91/128=91) (98=998)
	la def ilo_joball_how_actuall	91 "91 or more" 998 "don't know"
	la val ilo_joball_how_actual ilo_joball_how_actuall
recode ilo_how_hh_chores (27/38=27)
	la def ilo_how_hh_choresl	27 "27 or more"
	la val ilo_how_hh_chores ilo_how_hh_choresl

	drop e4aa_2digits e4bb_2digits h1ea_2digits h1fa_2digits h2db_2digits h2fb_2digits k2e1_2digits k2f1_2digits

sort anon_id07

saveold "$out\SPC_WSM_2022_LFS_Person_v01_PUF", version(12) replace

*	-------------------------------------------------------------------------------------------------------------------------------
**	6. Run the post-anonymization datasets through the Statistical Disclosure Control Shiny GUI
*	-------------------------------------------------------------------------------------------------------------------------------
*	COVER DATASET
** Scenario 1: Risk disclosure: 0.26% (7.23)

*	PERSON DATASET
** Scenario 1: Risk disclosure: % ()
** Scenario 2: Risk disclosure: % ()
** Scenario 3: Risk disclosure: % ()
** Scenario 4: Risk disclosure: 0.13% (20.8)

drop pers_id


