*	=========================================	*
*	Olivier Menaouer
*	SDD, SPC
*	12/11/2024
*	Palau 2023/24 HIES
*	Anonymization of datasets
*	========================================	*

clear all
version 15
set more off
tempfile temp1 temp2 temp3 temp4 temp5 temp6 temp7 temp8 temp9 temp10 temp 11 temp12
global in	"C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Palau\SPC_PLW_2023_HIES_v01_M\Data\Original\Data processing"
global TEMP	"C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Palau\SPC_PLW_2023_HIES_v01_M_v01_A_PUF\Data\Original"
global PUF	"C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Palau\SPC_PLW_2023_HIES_v01_M_v01_A_PUF\Data\Distribute"

set seed 12345

********************************************************************************
	
* Process for anonymisation:
	**	1. Remove direct identifiers (names, phone numbers, GPS coordinates, other) - already done;
	**	2. Anonymise the HH_ID, PSU, etc.;
	**	3. Identify "keys" in the dataset that may be used to identify individuals / households;
	**	4. Run the data set through the Statistical Disclosure Control Shiny GUI (http://www.ihsn.org/software/disclosure-control-toolbox);
	**	5. Anonymise the "keys" by top/bottom coding, aggregating, other;
	**	6. Run the dataset through the SDC micro app to establish if the risk has sufficiently reduced.
	
*	-------------------------------------------------------------------------------------------------------------------------------
**	1. Remove direct identifiers (names, phone numbers, GPS coordinates, other) - already done
*	-------------------------------------------------------------------------------------------------------------------------------
	***	Selecting cleaned, labelled and de-identified datasets
use "C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Palau\SPC_PLW_2023_HIES_v01_M\Data\Original\Data processing\Version 3\2_Person\p1_Profile", clear

collapse (count) hm_basic__id, by(interview__k)
	rename hm_basic__id hhsize
	la var hhsize "household size"
	save `temp1', replace

*	COVER DATASET	*
use "$in\Version 3\1_Cover\s0_Cover",clear

drop name__0 buildingGPS__Latitude buildingGPS__Longitude

*	-------------------------------------------------------------------------------------------------------------------------------
**	2. Anonymise the IDs: e.g.: HH_ID, PSU, Villages, ... etc.
*	-------------------------------------------------------------------------------------------------------------------------------
	** We need to keep hhid to merge with other data files
	** We need to keep the sampling weight
	** We need to keep EA if the sampling weights are to be reconstructed

	gen x = runiform()															// generate randon number for all households
	sort x																		// sort randomly
	gen anon_id07 = _n															// generate household ID that is in no particular order
	la var anon_id07 "anonymized household id"
	drop x
	
	gen rururb=.
		replace rururb=1 if inlist(states,7,14)
		replace rururb=2 if missing(rururb)
			la var rururb "area of residence"
			order rururb, a(hhld_id)
			
	merge 1:1 interview__k using "$in\Version 3\PLW_2023_HIES_Sample", keepusing(fweight)
		drop if _m!=3
		drop _m
		
	merge 1:1 interview__k using `temp1', keepusing(hhsize)
		drop if _m!=3
		drop _m

	** We would usually generate a random States number as this is an identifier
	preserve
		collapse (first) rururb, by(states)
		gen states_2=runiform()
		sort states_2
		gen anon_states=_n
		la var anon_states "anonymized states number"
		save `temp1', replace
	restore
	merge m:1 states using `temp1'
		assert _merge==3
		drop _merge
	
	** We would usually generate a random Hamlet number as this is an identifier
	preserve
		collapse (first) rururb, by(hamlet)
		gen hamlet_2=runiform()
		sort hamlet_2
		gen anon_hamlet=_n
		la var anon_hamlet "anonymized hamlet number"
		save `temp2', replace
	restore
	merge m:1 hamlet using `temp2'
		assert _merge==3
		drop _merge

**	Cleaning the new dataset
	sort anon_id07
	
	order anon_id07, after(interview__key)
	order anon_states, after(states)	
	order anon_hamlet, after(hamlet)
	
	drop oth_reason_replace
	
	saveold "$TEMP\SPC_PLW_2023_HIES_0-Cover_v02", version(12) replace

** Only keep relevant variables
	keep interview__key anon_id07 interview__id fweight hhld_id rururb anon_states anon_hamlet states hamlet dwell_type round hhsize reason_no_interview datetime_interview

** We now have all the anonymised variables that we need for the analysis
	** We keep this as a secure (not for public access) master merge file
		
	** Merge this with the other datafiles

		save `temp4', replace

*	1. DEMOGRAPHIC CHARACTERISTICS DATASET	*
use "$in\Version 3\2_Person\p1_Profile", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet states)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key

		save "$TEMP\SPC_PLW_2023_HIES_1-DemoCharacter_v02", replace

*	2. PERSON EDUCATION DATASET	*
use "$in\Version 3\2_Person\p2_education", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name

		save "$TEMP\SPC_PLW_2023_HIES_2-Education_v02", replace

*	3. PERSON HEALTH DATASET	*
use "$in\Version 3\2_Person\p3_Health", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name

		save "$TEMP\SPC_PLW_2023_HIES_3-Health_v02", replace

*	4. PERSON DISDABILITY DATASET	*
use "$in\Version 3\2_Person\p4_Functionality", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name

		save "$TEMP\SPC_PLW_2023_HIES_4-Functionality_v02", replace

*	5. PERSON COMMUNICATION DATASET	*
use "$in\Version 3\2_Person\p5_communication", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name

		save "$TEMP\SPC_PLW_2023_HIES_5-Communication_v02", replace

*	6. PERSON ALCOHOL DATASET	*
use "$in\Version 3\2_Person\p6_alcohol", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name

		save "$TEMP\SPC_PLW_2023_HIES_6-Alcohol_v02", replace

*	7. PERSON OTHER INDIV EXP DATASET	*
use "$in\Version 3\2_Person\p7_Other_ind_expenses", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name
	la var p7a1 "p7a1: spend on taxi/bus"

		save "$TEMP\SPC_PLW_2023_HIES_7-OtherIndivExp_v02", replace

*	8. PERSON LABOUR DATASET	*
use "$in\Version 3\2_Person\p8_LabourForce", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
		labvars p807 p808 "p807: work for income" "p808: help family business"
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name

		save "$TEMP\SPC_PLW_2023_HIES_8-LabourForce_v02", replace
		
*	*. PERSON FAFH DATASET	*
use "$in\Version 3\2_Person\p9_fafh", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb anon_states anon_hamlet, first
	sort anon_id07
	drop interview__key name

		save "$TEMP\SPC_PLW_2023_HIES_9-FAFH_v02", replace

*	11. HOUSEHOLD DWELLING DATASET	*
use "$in\Version 3\3_Household\h11_dwelling", clear
*	la var h1118 "h1128: main water source for cleaning"
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	la def h1114	1 "This household" 2 "Another household" 3 "Water free" 4 "Water payment included in rent", modify
		la val h1114 h1114
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_11-Dwelling_v02", replace

*	12. HOUSEHOLD ASSETS DATASET	*
use "$in\Version 3\3_Household\h12_assets", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_12-HouseholdAssets_v02", replace

*	13a. HOUSEHOLD HOME MAINTENANCE DATASET	*
use "$in\Version 3\3_Household\h13a_manitenance", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_13a-HomeMaintenance_v02", replace

*	13b. HOUSEHOLD VECHICLES DATASET	*
use "$in\Version 3\3_Household\h13b_vehicles", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_13b-Vehicles_v02", replace

*	13c. HOUSEHOLD INTERNAT TRAVEL DATASET	*
use "$in\Version 3\3_Household\h13c_int_travel", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_13c-InternationalTravel_v02", replace

*	13d. HOUSEHOLD DOMESTIC TRAVEL DATASET	*
use "$in\Version 3\3_Household\h13d_domestic_travel", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	foreach x of varlist coicop_13d6__1 coicop_13d7__1 coicop_13d8__1 coicop_13d9__1 coicop_13d10__1 coicop_13d6__2 coicop_13d7__2 coicop_13d8__2 coicop_13d9__2 coicop_13d10__2 coicop_13d6__3 coicop_13d7__3 coicop_13d8__3 coicop_13d9__3 coicop_13d10__3 coicop_13d6__4 coicop_13d7__4 coicop_13d8__4 coicop_13d9__4 coicop_13d10__4 coicop_13d6__5 coicop_13d7__5 coicop_13d8__5 coicop_13d9__5 coicop_13d10__5 coicop_13d6__6 coicop_13d7__6 coicop_13d8__6 coicop_13d9__6 coicop_13d10__6 coicop_13d6__7 coicop_13d7__7 coicop_13d8__7 coicop_13d9__7 coicop_13d10__7 coicop_13d6__8 coicop_13d7__8 coicop_13d8__8 coicop_13d9__8 coicop_13d10__8 coicop_13d6__9 coicop_13d7__9 coicop_13d8__9 coicop_13d9__9 coicop_13d10__9 coicop_13d6__10 coicop_13d7__10 coicop_13d8__10 coicop_13d9__10 coicop_13d10__10 coicop_13d6__11 coicop_13d7__11 coicop_13d8__11 coicop_13d9__11 coicop_13d10__11 coicop_13d6__12 coicop_13d7__12 coicop_13d8__12 coicop_13d9__12 coicop_13d10__12  {
	la var `x' "coicop code"
	}
	foreach x of varlist desc_13d6__1 desc_13d7__1 desc_13d8__1 desc_13d9__1 desc_13d10__1 desc_13d6__2 desc_13d7__2 desc_13d8__2 desc_13d9__2 desc_13d10__2 desc_13d6__3 desc_13d7__3 desc_13d8__3 desc_13d9__3 desc_13d10__3 desc_13d6__4 desc_13d7__4 desc_13d8__4 desc_13d9__4 desc_13d10__4 desc_13d6__5 desc_13d7__5 desc_13d8__5 desc_13d9__5 desc_13d10__5 desc_13d6__6 desc_13d7__6 desc_13d8__6 desc_13d9__6 desc_13d10__6 desc_13d6__7 desc_13d7__7 desc_13d8__7 desc_13d9__7 desc_13d10__7 desc_13d6__8 desc_13d7__8 desc_13d8__8 desc_13d9__8 desc_13d10__8 desc_13d6__9 desc_13d7__9 desc_13d8__9 desc_13d9__9 desc_13d10__9 desc_13d6__10 desc_13d7__10 desc_13d8__10 desc_13d9__10 desc_13d10__10 desc_13d6__11 desc_13d7__11 desc_13d8__11 desc_13d9__11 desc_13d10__11 desc_13d6__12 desc_13d7__12 desc_13d8__12 desc_13d9__12 desc_13d10__12 {
	la var `x' "item description"
	}		
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_13d-DomesticTravel_v02", replace

*	13e. HOUSEHOLD SERVICES DATASET	*
use "$in\Version 3\3_Household\h13e_hh_service_tax", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key

		save "$TEMP\SPC_PLW_2023_HIES_13e-HouseholdServices_v02", replace

*	13f. HOUSEHOLD FINANCIAL SUPPORT DATASET	*
use "$in\Version 3\3_Household\h13f_financial_support", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_13f-FinancialSupport_v02", replace

*	13g. HOUSEHOLD OTHER HH EXP DATASET	*
use "$in\Version 3\3_Household\h13g_hh_oth_expenditure", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key

		save "$TEMP\SPC_PLW_2023_HIES_13g-OtherHhldExpenditure_v02", replace

*	14. HOUSEHOLD CEREMONIES DATASET	*
use "$in\Version 3\3_Household\h14_ceremonies", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_14-Ceremonies_v02", replace

*	15. HOUSEHOLD REMITTANCES DATASET	*
use "$in\Version 3\3_Household\h15_remittances", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	foreach x of varlist paccoi_1506_1 paccoi_1506_2 paccoi_1506_3 paccoi_1506_4 paccoi_1506_5 {
	la var `x' "paccoi code"
	}
	foreach x of varlist desc_1506_1 desc_1506_2 desc_1506_3 desc_1506_4 desc_1506_5 {
	la var `x' "item description"
	}
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_15-Remittances_v02", replace

*	16. HOUSEHOLD FIES DATASET	*
use "$in\Version 3\3_Household\h16_Food_insecurity", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_16-FoodInsecurity_v02", replace

*	17. HOUSEHOLD FISHERIES DATASET	*
use "$in\Version 3\3_Household\h17_Fisheries", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	labvars h1702__0 h1702__1 h1702__2 h1702__3 "member engaged in fishing - 1" "member engaged in fishing - 2" "member engaged in fishing - 3" "member engaged in fishing - 4"
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_17-Fisheries_v02", replace

*	18. HOUSEHOLD LIVESTOCK AQUAC DATASET	*
use "$in\Version 3\3_Household\h18_livestock", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_18-Livestock_v02", replace

*	19. HOUSEHOLD AGRIC DATASET	*
use "$in\Version 3\3_Household\h19_Agriculture", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_19-Agriculture_v02", replace

*	20. HOUSEHOLD HANDICRAFT DATASET	*
use "$in\Version 3\3_Household\h20_Handicrafts", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_20-Handicraft_v02", replace

*	21. HOUSEHOLD HORTICULTURE DATASET	*
use "$in\Version 3\3_Household\h21_Horticulture", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_PLW_2023_HIES_21-Horticulture_v02", replace

*	22. HOUSEHOLD HUNTING DATASET	*
use "$in\Version 3\3_Household\h22_Hunting", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key

		save "$TEMP\SPC_PLW_2023_HIES_22-Hunting_v02", replace

*	23. HOUSEHOLD LEGAL SERVICES DATASET	*
use "$in\Version 3\3_Household\h23_Legal_services", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key

		save "$TEMP\SPC_PLW_2023_HIES_23-LegalServices_v02", replace
		
*	30. HOUSEHOLD EXPENDITURE DATASET	*
use "$in\Version 6\ExpenditureAggregates_v6", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	bysort interview__key : gen temp=_n
		encode interview__key, gen(temp2)
		egen exp_id=concat(temp2 temp), punct(.)								//	one single identifier variable is required in sdcMicro so concatenate of household ID + transaction nb
		drop temp temp2
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_PLW_2023_HIES_30-ExpenditureAggregate_v02", replace

*	40. HOUSEHOLD INCOME DATASET	*
use "$in\Version 6\IncomeAggregates_v6", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight rururb anon_states anon_hamlet hhsize)
		drop if _m!=3 & fweight==.
		assert _merge==3
		drop _merge
	bysort interview__key : gen temp=_n
		encode interview__key, gen(temp2)
		egen exp_id=concat(temp2 temp), punct(.)								//	one single identifier variable is required in sdcMicro so concatenate of household ID + transaction nb
		drop temp temp2
	order anon_id07 fweight rururb anon_states anon_hamlet hhsize, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_PLW_2023_HIES_40-IncomeAggregate_v02", replace

use `temp4', clear
	drop interview__key interview__id states hamlet hhld_id
	save "$PUF\SPC_PLW_2023_HIES_0-Cover_v01_PUF", replace

*	-------------------------------------------------------------------------------------------------------------------------------
**	3. Identify "keys" in the dataset that may be used to identify individuals / households
*	-------------------------------------------------------------------------------------------------------------------------------
*	0. COVER DATASET	*
*		Scenario 1: 
*		Scenario 2: 

*	XX. PERSON RECORD DATASET		*
*		Scenario 1: sex age states p208 p301 p302 p811 p813
*		Scenario 2: age_grp5 p112 anon_p302 p801 p802 ilo_job1_how_actual_bands
*		Scenario 3: sex age p112 p208 p301 p801 p853
	
*	XX. HOUSEHOLD RECORD DATASET			*
*		Scenario 1: hhsize states h1105 h1106 h1202__17 h13b2__1 h1707 h1802__3
*		Scenario 2: 
		
*	30. HOUSEHOLD EXPENDITURE DATASET	*
*		Scenario 1: strata hhsize coicop
*		Scenario 2: 

*	40. HOUSEHOLD INCOME DATASET	*
*		Scenario 1: strata hhsize paccoi isco isic
*		Scenario 2: 

*	-------------------------------------------------------------------------------------------------------------------------------
**	4. Run the pre-anonymization datasets through the Statistical Disclosure Control Shiny GUI
*	-------------------------------------------------------------------------------------------------------------------------------
*	0. COVER DATASET	*
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: % ()

*	AGG PERSON DATASET
*		Risk of disclosure 1: 33.63% (780.24)
*		Risk of disclosure 2: 28% ()
*		Risk of disclosure 3: 30.2%

*	AGG HOUSEHOLD DATASET
*		Risk of disclosure 1: 22.13% (209.11)
*		Risk of disclosure 2: % ()
	
*	30. HOUSEHOLD EXPENDITURE DATASET	*
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: % ()

*	40. HOUSEHOLD INCOME DATASET	*
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: % ()

*	-------------------------------------------------------------------------------------------------------------------------------
**	5. Anonymise the "keys" by top/bottom coding, aggregating, other;
*	-------------------------------------------------------------------------------------------------------------------------------
*	COVER DATASET	*
** Preliminary cleaning
	label data "s0 cover dataset"

** Recoding variables and recode the "0" as missing (.)
* 1 variable anonymized : hhsize
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel
	
saveold "$PUF\SPC_PLW_2023_HIES_0-Cover_v01_PUF", version(12) replace
			
*	1. PERSON DEMOG CHARACT DATASET
use "$TEMP\SPC_PLW_2023_HIES_1-DemoCharacter_v02",clear

** Preliminary cleaning
	label data "s1 demographic characteristics dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 3 variables anonymized : age p105 p112
	drop age_grp5 p112n states name
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
			drop age
	recode p105 (9 10 = 13)
	recode p112 (3 5=10)

saveold "$PUF\SPC_PLW_2023_HIES_1-DemoCharacter_v01_PUF", version(12) replace
			
*	2. PERSON EDUCATION DATASET
use "$TEMP\SPC_PLW_2023_HIES_2-Education_v02",clear

** Preliminary cleaning
	label data "s2 education dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 2 variables anonymized : p208 211
	drop age_grp5 p105 states hamlet interview__id
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
			drop age
	recode p208 (18 19 21=18) (22 31=22)
		la def p208	18 "University degree" 22 "Special school / no school completed", modify
		la val p208 p208
	recode p211 (12 13=12) (18 19 21=18) (22 31=31)
		la def p211	12 "High school" 18 "University degree" 22 "Special school / no school completed", modify
		la val p211 p211

saveold "$PUF\SPC_PLW_2023_HIES_2-Education_v01_PUF", version(12) replace

*	3. PERSON HEALTH DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_3-Health_v02",clear

** Preliminary cleaning
	label data "s3 health dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 5 variables anonymized : p301 p302 p304__5 p304__6 p304__13
	drop p105 p304an p320n states hamlet interview__id age_grp5
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
		drop age
	
	recode p301 (0/2=2) (6/6.5=6), gen(anon_p301)
		label define p301l				2 "2 feet or less"  6 "6 feet or more" 997 "Not available" 998 "Disabled" 999 "Refused"
		label values anon_p301 p301l
			order anon_p301, a(p301)
			la var anon_p301 "recode of p301: height in feet and inches"
			drop p301
	recode p302 (6/15=15) (280/420=280) (1170 1640=280), gen(anon_p302_2)
		gen anon_p302=round(anon_p302_2,1.0)
			la def	p302l	15 "Less than 15lbs" 280 "280lbs or more" 997 "Not available" 998 "Disabled" 999 "Refused"
			la val anon_p302 p302l
			order anon_p302,a(p302)
			la var anon_p302 "recode of p302: weight in lbs"
			drop p302
	gen p304_5__6=0
		replace p304_5__6=1 if (p304__5==1 | p304__6==1)
		la var p304_5__6 "recode of p304_5 and p304_6: chronic bronchite or liver disease"
		order p304_5__6, a(p304__6)
		drop p304__5 p304__6
	gen p304_13__14=0
		replace p304_13__14=1 if (p304__13==1 | p304__14==1)
		la var p304_13__14 "recode of p304_13 and p304_14: tuberculosis or other disease"
		order p304_13__14, a(p304__14)
		drop p304__13

saveold "$PUF\SPC_PLW_2023_HIES_3-Health_v01_PUF", version(12) replace

*	4. PERSON DISABILITY DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_4-Functionality_v02",clear

** Preliminary cleaning
	label data "s4 functionality dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop age_grp5 states hamlet
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_PLW_2023_HIES_4-Functionality_v01_PUF", version(12) replace

*	5. PERSON COMMUNICATION DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_5-Communication_v02",clear

** Preliminary cleaning
	label data "s5 communication dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop age_grp5 states hamlet p502n
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_PLW_2023_HIES_5-Communication_v01_PUF", version(12) replace

*	6. PERSON ALCOHOL DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_6-Alcohol_v02",clear

** Preliminary cleaning
	label data "s6 alcohol dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop age_grp5 states hamlet p602cn__1 p602cn__2 p602cn__3 p602cn__4 p602cn__5 p602cn__6 p602cn__7 p602cn__8 p602cn__9 p601n
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_PLW_2023_HIES_6-Alcohol_v01_PUF", version(12) replace

*	7. PERSON OTHER INDIV EXP DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_7-OtherIndivExp_v02",clear

** Preliminary cleaning
	label data "s7 other individual expenditure dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop age_grp5 states hamlet p7b1n p7c1n
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_PLW_2023_HIES_7-OtherIndivExp_v01_PUF", version(12) replace

*	8. PERSON LABOUR DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_8-LabourForce_v02",clear

** Preliminary cleaning
	label data "s8 labour dataset - individual"
	
** Recoding variables and recode the "0" as missing (.)
* 11 variables anonymized : p801 p802	p810a p811 p813 p819 p822 p844 p845 p852 p853 
	drop age_grp5 states hamlet p830n p836n p843n p850n p858c p860n p208
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

	recode p801 (6 7=6)
		la def p801	6 "internship / voluntary work", modify
		la val p801 p801
	recode p802	(1 2 4=1) (8 10 11=8)
		la def p802	1 "fishing" 8 "agriculture", modify
		la val p802	p802
	drop p810a																	// too risky
	drop p811 p812 p812_2														// we only keep ISIC at 1 digit level
	drop p813 p814 p814_2														// we only keep ISCO at 1 digit level
	recode p819 (3 4=3)
		la def p819 3 "Don't know / do not pay income taxes", modify
		la val p819  p819
	recode p822 (2 3=2)
		la def p822	2 "Cooperation or Limited partnership", modify
		la val p822 p822
		
	*	Aggregating ISIC and ISCO for secondary activity	
	**	ISIC
	***	2-digit level
	gen ilo_job2_eco_isic4_2digits=. 
	    replace ilo_job2_eco_isic4_2digits=int(p845/100) if p842==1
	    replace ilo_job2_eco_isic4_2digits=. if p842!=1
                lab var ilo_job2_eco_isic4_2digits "Economic activity (ISIC Rev. 4), 2 digits level - second job"

	* 1-digit level
    gen ilo_job2_eco_isic4=.
	    replace ilo_job2_eco_isic4=1 if inrange(ilo_job2_eco_isic4_2digits,1,3)
	    replace ilo_job2_eco_isic4=2 if inrange(ilo_job2_eco_isic4_2digits,5,9)
	    replace ilo_job2_eco_isic4=3 if inrange(ilo_job2_eco_isic4_2digits,10,33)
	    replace ilo_job2_eco_isic4=4 if ilo_job2_eco_isic4_2digits==35
	    replace ilo_job2_eco_isic4=5 if inrange(ilo_job2_eco_isic4_2digits,36,39)
	    replace ilo_job2_eco_isic4=6 if inrange(ilo_job2_eco_isic4_2digits,41,43)
	    replace ilo_job2_eco_isic4=7 if inrange(ilo_job2_eco_isic4_2digits,45,47)
	    replace ilo_job2_eco_isic4=8 if inrange(ilo_job2_eco_isic4_2digits,49,53)
	    replace ilo_job2_eco_isic4=9 if inrange(ilo_job2_eco_isic4_2digits,55,56)
	    replace ilo_job2_eco_isic4=10 if inrange(ilo_job2_eco_isic4_2digits,58,63)
	    replace ilo_job2_eco_isic4=11 if inrange(ilo_job2_eco_isic4_2digits,64,66)
	    replace ilo_job2_eco_isic4=12 if ilo_job2_eco_isic4_2digits==68
	    replace ilo_job2_eco_isic4=13 if inrange(ilo_job2_eco_isic4_2digits,69,75)		
	    replace ilo_job2_eco_isic4=14 if inrange(ilo_job2_eco_isic4_2digits,77,82)
	    replace ilo_job2_eco_isic4=15 if ilo_job2_eco_isic4_2digits==84
        replace ilo_job2_eco_isic4=16 if ilo_job2_eco_isic4_2digits==85
	    replace ilo_job2_eco_isic4=17 if inrange(ilo_job2_eco_isic4_2digits,86,88)
	    replace ilo_job2_eco_isic4=18 if inrange(ilo_job2_eco_isic4_2digits,90,93)
	    replace ilo_job2_eco_isic4=19 if inrange(ilo_job2_eco_isic4_2digits,94,96)
	    replace ilo_job2_eco_isic4=20 if inrange(ilo_job2_eco_isic4_2digits,97,98)
	    replace ilo_job2_eco_isic4=21 if ilo_job2_eco_isic4_2digits==99
	    replace ilo_job2_eco_isic4=22 if ilo_job2_eco_isic4==. & p842==1
		        lab val ilo_job2_eco_isic4 eco_isic4_1digit
			    lab var ilo_job2_eco_isic4 "Economic activity (ISIC Rev. 4) - second job"
				order ilo_job2_eco_isic4, a(p845)
		        lab def eco_isic4_1digit 1 "A - Agriculture, forestry and fishing"	2 "B - Mining and quarrying"	3 "C - Manufacturing"	4 "D - Electricity, gas, steam and air conditioning supply" ///
                                         5 "E - Water supply; sewerage, waste management and remediation activities"	6 "F - Construction"	7 "G - Wholesale and retail trade; repair of motor vehicles and motorcycles"	8 "H - Transportation and storage" ///
                                         9 "I - Accommodation and food service activities"	10 "J - Information and communication"	11 "K - Financial and insurance activities"	12 "L - Real estate activities" ///
                                         13 "M - Professional, scientific and technical activities"	14 "N - Administrative and support service activities"	15 "O - Public administration and defence; compulsory social security"	16 "P - Education" ///
                                         17 "Q - Human health and social work activities"	18 "R - Arts, entertainment and recreation"	19 "S - Other service activities"	20 "T - Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use" ///
                                         21 "U - Activities of extraterritorial organizations and bodies"	22 "X - Not elsewhere classified"		
  	  		    lab val ilo_job2_eco_isic4 eco_isic4_1digit
	
	** ISCO
	*** 2-digit level
    gen ilo_job2_ocu_isco08_2digits=. 
	    replace ilo_job2_ocu_isco08_2digits=int(p847/100) if p842==1
		replace ilo_job2_ocu_isco08_2digits=. if p842!=1
	            lab var ilo_job2_ocu_isco08_2digits "Occupation (ISCO-08), 2 digit level - second job"
				
	***	1-digit level 				
	gen ilo_job2_ocu_isco08=.
	    replace ilo_job2_ocu_isco08=11 if inlist(ilo_job2_ocu_isco08_2digits,.) & p842==1                         // Not elsewhere classified
		replace ilo_job2_ocu_isco08=int(ilo_job2_ocu_isco08_2digits/10) if (ilo_job2_ocu_isco08==. & p842==1)     // The rest of the occupations
		replace ilo_job2_ocu_isco08=10 if (ilo_job2_ocu_isco08==0 & p842==1)                                      // Armed forces
				lab val ilo_job2_ocu_isco08 ocu_isco08_1digit
				lab var ilo_job2_ocu_isco08 "Occupation (ISCO-08) - second job"
				order ilo_job2_ocu_isco08, a(p847)
		        lab def ocu_isco08_1digit 1 "1 - Managers"	2 "2 - Professionals"	3 "3 - Technicians and associate professionals"	4 "4 - Clerical support workers" ///
                                          5 "5 - Service and sales workers"	6 "6 - Skilled agricultural, forestry and fishery workers"	7 "7 - Craft and related trades workers"	8 "8 - Plant and machine operators, and assemblers"	///
                                          9 "9 - Elementary occupations"	10 "0 - Armed forces occupations"	11 "X - Not elsewhere classified"		
				lab val ilo_job2_ocu_isco08 ocu_isco08_1digit
				
	drop p844 p845 p846 p847 ilo_job2_eco_isic4_2digits ilo_job2_ocu_isco08_2digits
	
	*	Hours ACTUALLY worked
	**	Main job
	gen ilo_job1_how_actual=.
	    replace ilo_job1_how_actual=p852 if inrange(p852,0,168)
		        lab var ilo_job1_how_actual "Weekly hours actually worked - main job"
		
    gen ilo_job1_how_actual_bands=.
	    replace ilo_job1_how_actual_bands=1 if ilo_job1_how_actual==0
	    replace ilo_job1_how_actual_bands=2 if ilo_job1_how_actual>=1 & ilo_job1_how_actual<=14
	    replace ilo_job1_how_actual_bands=3 if ilo_job1_how_actual>14 & ilo_job1_how_actual<=29
	    replace ilo_job1_how_actual_bands=4 if ilo_job1_how_actual>29 & ilo_job1_how_actual<=34
	    replace ilo_job1_how_actual_bands=5 if ilo_job1_how_actual>34 & ilo_job1_how_actual<=39
	    replace ilo_job1_how_actual_bands=6 if ilo_job1_how_actual>39 & ilo_job1_how_actual<=48
	    replace ilo_job1_how_actual_bands=7 if ilo_job1_how_actual>48 & ilo_job1_how_actual!=.
	    replace ilo_job1_how_actual_bands=. if missing(ilo_job1_how_actual_bands)
		   	    lab def how_bands_act 1 "No hours actually worked" 2 "01-14" 3 "15-29" 4 "30-34" 5 "35-39" 6 "40-48" 7 "49+" 8 "Not elsewhere classified"		
				lab val ilo_job1_how_actual_bands how_bands_act
				lab var ilo_job1_how_actual_bands "Weekly hours actually worked bands - main job"
				order ilo_job1_how_actual_bands, a(p852)
	
    ** Secondary job
    gen ilo_job2_how_actual=.
	    replace ilo_job2_how_actual= p853      if inrange(p853,0,168)
	            lab var ilo_job2_how_actual "Weekly hours actually worked - second job"
		
	gen ilo_job2_how_actual_bands=.
	    replace ilo_job2_how_actual_bands=1 if ilo_job2_how_actual==0
		replace ilo_job2_how_actual_bands=2 if ilo_job2_how_actual>=1 & ilo_job2_how_actual<=14
		replace ilo_job2_how_actual_bands=3 if ilo_job2_how_actual>14 & ilo_job2_how_actual<=29
		replace ilo_job2_how_actual_bands=4 if ilo_job2_how_actual>29 & ilo_job2_how_actual<=34
		replace ilo_job2_how_actual_bands=5 if ilo_job2_how_actual>34 & ilo_job2_how_actual<=39
		replace ilo_job2_how_actual_bands=6 if ilo_job2_how_actual>39 & ilo_job2_how_actual<=48
		replace ilo_job2_how_actual_bands=7 if ilo_job2_how_actual>48 & ilo_job2_how_actual!=.
		replace ilo_job2_how_actual_bands=8 if ilo_job2_how_actual_bands==. & p842==1
		replace ilo_job2_how_actual_bands=. if p842!=1
		   	    lab val ilo_job2_how_actual_bands how_bands_act
				lab var ilo_job2_how_actual_bands "Weekly hours actually worked bands - second job"
				order ilo_job2_how_actual_bands, a(p853)
	drop p852 p853 ilo_job1_how_actual ilo_job2_how_actual

saveold "$PUF\SPC_PLW_2023_HIES_8-Labour_v01_PUF", version(12) replace

*	9. PERSON FAFH DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_9-FAFH_v02",clear

** Preliminary cleaning
	label data "s9 food consumed away from home (fafh) dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop age_grp5 states hamlet p208
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_PLW_2023_HIES_9-FAFH_v01_PUF", version(12) replace

*	AGG PERSON DATASET
**	Original	**
use "$TEMP\SPC_PLW_2023_HIES_1-DemoCharacter_v02", clear

	keep anon_id07 hm_basic__id fweight sex age p105 p112 rururb states anon_states anon_hamlet 
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_PLW_2023_HIES_2-Education_v02",				keepusing(p208 p211)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_PLW_2023_HIES_3-Health_v02",					keepusing(p301 p302 p304__5 p304__6 p304__13)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_PLW_2023_HIES_8-LabourForce_v02",				keepusing(p801 p802	p810a p811 p813 p819 p822 p844 p845 p852 p853)
		drop _m
	
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id

saveold "$TEMP\SPC_PLW_2023_HIES_PersonAgg1-9_v02", version(12) replace
	
**	PUF	**	
use "$PUF\SPC_PLW_2023_HIES_1-DemoCharacter_v01_PUF", clear

	keep anon_id07 hm_basic__id fweight sex age_gr p105 p112 rururb anon_states anon_hamlet 
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_PLW_2023_HIES_2-Education_v01_PUF",			keepusing(p208 p211)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_PLW_2023_HIES_3-Health_v01_PUF",				keepusing(anon_p301 anon_p302 p304_5__6 p304_13__14)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_PLW_2023_HIES_8-Labour_v01_PUF",				keepusing(p801 p802 p812_1 p814_1 p819 p822 ilo_job2_eco_isic4 ilo_job2_ocu_isco08 ilo_job1_how_actual_bands ilo_job2_how_actual_bands)
		drop _m

	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id

saveold "$PUF\SPC_PLW_2023_HIES_PersonAgg1-9_v01_PUF", version(12) replace
	
*	11. HOUSEHOLD DWELLING DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_11-Dwelling_v02",clear

** Preliminary cleaning
	label data "s11 dwelling characteristics dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 4 variables anonymized : h1101 h1105 h1106 h1117
	drop h1101n h1102n h1103n h1104n h1107n h1108n h1109n h1110n states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel
	
	recode h1101 (4 5=4)
		la def h1101	4 "A building with 10 or more apartments", modify
		la val h1101 h1101
	recode h1105 (9/max=9)
		la def h1105l	9 "9 or more"
		la val h1105 h1105l
	recode h1106 (1945/1979=1) (1980/1994=2) (1995/2004=3) (2005/2014=4) (2015/2019=5) (2020/2023=6)
		la def h1106l	1 "Before 1980" 2 "1980-1994" 3 "1995-2004" 4 "2005-2014" 5 "2015-2019" 6 "2020-2023" 9999 "Unknown"
		la val h1106 h1106l
	recode h1117 (3 4 5=3)
		la def h1117	3 "public piped water to neighbor/shared tap/borehole", modify
		la val h1117 h1117

saveold "$PUF\SPC_PLW_2023_HIES_11-Dwelling_v01_PUF", version(12) replace

*	12. HOUSEHOLD ASSETS DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_12-HouseholdAssets_v02",clear

** Preliminary cleaning
	label data "s12 household assets dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 11 variables anonymized : h1202__1 h1202__3 h1202__4 h1202__6 h1202__7 h1202__15 h1202__17 h1202__25 h1202__27 h1202__36 h1202__38
	drop h1201n1 h1201n2 h1201n3 h1201n5 h1201n4 states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel
       
	recode h1202__1 (6/30=6)
		la def h1202__1			6 "6 or more"
		la val h1202__1 h1202__1
	recode h1202__3 (7/12=7)
		la def h1202__3			7 "7 or more"
		la val h1202__3 h1202__3
	recode h1202__4 (6/18=6)
		la def h1202__4			6 "6 or more"
		la val h1202__4 h1202__4
	recode h1202__6 (3/7=3)
		la def h1202__6			3 "3 or more"
		la val h1202__6 h1202__6
	recode h1202__7 (3/7=3)
		la def h1202__7			3 "3 or more"
		la val h1202__7 h1202__7
	recode h1202__15 (5/14=5)
		la def h1202__15		5 "5 or more"
		la val h1202__15 h1202__15
	recode h1202__17 (6/14=6)
		la def h1202__17		6 "6 or more"
		la val h1202__17 h1202__17
	recode h1202__25 (7/10=7)
		la def h1202__25		7 "7 or more"
		la val h1202__25 h1202__25
	recode h1202__27 (4 10=4)
		la def h1202__27		4 "4 or more"
		la val h1202__27 h1202__27
	recode h1202__36 (4 5=4)
		la def h1202__36		4 "4 or more"
		la val h1202__36 h1202__36
	recode h1202__38 (10/40=10)
		la def h1202__38		10 "10 or more"
		la val h1202__38 h1202__38

saveold "$PUF\SPC_PLW_2023_HIES_12-HouseholdAssets_v01_PUF", version(12) replace

*	13a. HOUSEHOLD HOME MAINTENANCE DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_13a-HomeMaintenance_v02",clear

** Preliminary cleaning
	label data "s13a home maintenance dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop h13a1n states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_13a-HomeMaintenance_v01_PUF", version(12) replace

*	13b. HOUSEHOLD VECHICLES DATASET	*

use "$TEMP\SPC_PLW_2023_HIES_13b-Vehicles_v02",clear

** Preliminary cleaning
	label data "s13b vehicles dataset - household"
	
** Recoding variables and recode the "0" as missing (.)
* 3 variable anonymized : h13b2__1 h13b2__2 h13b2__4 h13b2__5
	drop h13b1n states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel
 
	recode h13b2__1 (5/8=5)
		la def h13b2__1l			5 "5 or more"
		la val h13b2__1 h13b2__1l
	drop h13b2__2 																// too risky
	recode h13b2__4 (3/10=3)
		la def h13b2__4l			3 "3 or more"
		la val h13b2__4 h13b2__4l
	recode h13b2__5 (2/4=2)
		la def h13b2__5l			2 "2 or more"
		la val h13b2__5 h13b2__5l

saveold "$PUF\SPC_PLW_2023_HIES_13b-Vehicles_v01_PUF", version(12) replace

*	13c. HOUSEHOLD INTERNAT TRAVEL DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_13c-InternationalTravel_v02",clear

** Preliminary cleaning
	label data "s13c international travel dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 6 variables anonymized : h13c3__1 h13c3__2 h13c3__3 h13c3__4 h13c3__5 h13c3__6
	drop h13c3n__1 h13c4n__1 h13c3n__2 h13c4n__2 h13c3n__3 h13c4n__3 h13c3n__4 h13c4n__4 h13c3n__5 h13c4n__5 h13c3n__6 h13c4n__6 states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

	drop h13c3__1 h13c3__2 h13c3__3 h13c3__4 h13c3__5 h13c3__6
	
saveold "$PUF\SPC_PLW_2023_HIES_13c-InternationalTravel_v01_PUF", version(12) replace

*	13d. HOUSEHOLD DOMESTIC TRAVEL DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_13d-DomesticTravel_v02",clear

** Preliminary cleaning
	label data "s13d domestic travel dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop states h13d4n__1 h13d4n__2 h13d4n__3 h13d4n__4 h13d4n__5 h13d4n__6 h13d4n__7 h13d4n__8 h13d4n__9 h13d4n__10 h13d4n__11 h13d4n__12
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_13d-DomesticTravel_v01_PUF", version(12) replace

*	13e. HOUSEHOLD SERVICES DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_13e-HouseholdServices_v02",clear

** Preliminary cleaning
	label data "s13e household services dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop states h13e1n1 h13e1n2 h13e1n3
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_13e-HouseholdServices_v01_PUF", version(12) replace

*	13f. HOUSEHOLD FINANCIAL SUPPORT DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_13f-FinancialSupport_v02",clear

** Preliminary cleaning
	label data "s13f financial support dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop states h13f2n__1 h13f2n__2 h13f2n__3 h13f2n__4 h13f2n__5 h13f2n__6 h13f1n
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_13f-FinancialSupport_v01_PUF", version(12) replace

*	13g. HOUSEHOLD OTHER HH EXP DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_13g-OtherHhldExpenditure_v02",clear

** Preliminary cleaning
	label data "s13g other household expenditure dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop h13g1n1 h13g1n2 states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_13g-OtherHhldExp_v01_PUF", version(12) replace

*	14. HOUSEHOLD CEREMONIES DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_14-Ceremonies_v02",clear

** Preliminary cleaning
	label data "s14 ceremonies dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_14-Ceremonies_v01_PUF", version(12) replace

*	15. HOUSEHOLD REMITTANCES DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_15-Remittances_v02",clear

** Preliminary cleaning
	label data "s15 remittances dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop states h1503n__1 h1503n__2 h1503n__3 h1503n__4 h1503n__5
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_15-Remittances_v01_PUF", version(12) replace

*	16. HOUSEHOLD FIES DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_16-FoodInsecurity_v02",clear

** Preliminary cleaning
	label data "s16 FIES dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop states
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_16-FIES_v01_PUF", version(12) replace

*	17. HOUSEHOLD FISHERIES DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_17-Fisheries_v02",clear

** Preliminary cleaning
	label data "s17 fisheries dataset - household"
	
** Recoding variables and recode the "0" as missing (.)
* 2 variables anonymized : h1707 h1707a
	drop states h1706n__1 h1706n__2 h1706n__3 h1706n__4 h1706n__5 h1706n__6 h1706n__8 h1706n__9 h1703n h1722n1 h1722n2 h1722n3 h1735n h1706an__1 h1706an__2 h1706an__3 h1706an__4 h1706an__5 h1706an__6 h1706an__8 h1706an__9
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

	recode h1707 (60/180=60)
		la def h1707l	60 "60 hours or more"
		la val h1707 h1707l
	recode h1707a (16/160=16)
		la def h1707al	16 "16 or more"
		la val h1707a h1707al

saveold "$PUF\SPC_PLW_2023_HIES_17-Fisheries_v01_PUF", version(12) replace

*	18. HOUSEHOLD LIVESTOCK AQUAC DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_18-Livestock_v02",clear

** Preliminary cleaning
	label data "s18 livestock aquaculture dataset - household"
	
** Recoding variables and recode the "0" as missing (.)
* 6 variables anonymized : h1802__3 h1802__5 h1802__6
	drop states no_livestock h1801n1
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

	recode h1802__3 (3/9=3)
		la def h1802__3l			3 "3 or more"
		la val h1802__3 h1802__3l
	recode h1802__5 (10/230=10)
		la def h1802__5l			10 "10 or more"
		la val h1802__5 h1802__5l
	drop h1802__6 																// too risky
	drop h1805__3  																// too risky
	drop h1805__5  																// too risky
	drop h1805__6 																// too risky

saveold "$PUF\SPC_PLW_2023_HIES_18-Livestock_v01_PUF", version(12) replace

*	19. HOUSEHOLD AGRIC DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_19-Agriculture_v02",clear

** Preliminary cleaning
	label data "s19 agriculture dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 21 variable anonymized : h1907__2 h1907__3 h1907__6 h1907__9 h1907__10 h1907__12 h1907__13 h1912__1 h1912__2 h1912__3 h1912__4 h1917__1 h1917__2 h1917__3 h1908__13 h1913__1 h1913__2 h1913__3 h1918__1 h1918__2 h1918__9 
	drop states h1906n h1911n h1916n
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel
	
	recode h1907__2 (10/50=10)
		la def h1907__2l	10 "10 or more"
		la val h1907__2  h1907__2l
	recode h1907__3 (10/65=10)
		la def h1907__3l	10 "10 or more"
		la val h1907__3  h1907__3l
	recode h1907__6 (10/500=10)
		la def h1907__6l	10 "10 or more"
		la val h1907__6  h1907__6l
	recode h1907__9 (10/60=10)
		la def h1907__9l	10 "10 or more"
		la val h1907__9  h1907__9l
	recode h1907__10 (80=.)														// too risky
	recode h1907__12 (12/100=12)
		la def h1907__12l	12 "12 or more"
		la val h1907__12 h1907__12l
	recode h1907__13 (100=.)													// too risky
	recode h1912__1 (20/300=20)
		la def h1912__1l	20 "20 or more"
		la val h1912__1 h1912__1l
	recode h1912__2 (21/400=21)
		la def h1912__2l	21 "21 or more"
		la val h1912__2 h1912__2l
	recode h1912__3 (10/400=10)
		la def h1912__3l	10 "10 or more"
		la val h1912__3  h1912__3l
	recode h1912__4 (10/50=10)
		la def h1912__4l	10 "10 or more"
		la val h1912__4  h1912__4l
	recode h1917__1 (35/2600=35)
		la def h1917__1l	35 "35 or more"
		la val h1917__1  h1917__1l
	recode h1917__2 (8/300=8)
		la def h1917__2l	8 "8 or more"
		la val h1917__2  h1917__2l
	recode h1917__3 (100=.)														// too risky
	recode h1917__4 (10/100=10)
		la def h1917__4l	10 "10 or more"
		la val h1917__4  h1917__4l	
	recode h1908__13 (100=.)													// too risky
	recode h1913__1 (300=.)
	recode h1913__2 (30/200=30)
		la def h1913__2l	30 "30 or more"
		la val h1913__2 h1913__2l
	recode h1913__3 (20/250=20)
		la def h1913__3l	20 "20 or more"
		la val h1913__3 h1913__3l
	recode h1918__1 (15/400=15)
		la def h1918__1l	15 "15 or more"
		la val h1918__1 h1918__1l
	recode h1918__2 (50 300=.)													// too risky
	recode h1918__9 (50=.)														// too risky
	
saveold "$PUF\SPC_PLW_2023_HIES_19-Agriculture_v01_PUF", version(12) replace

*	20. HOUSEHOLD HANDICRAFT DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_20-Handicraft_v02",clear

** Preliminary cleaning
	label data "s20 handicraft dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized :  
	drop states h2002n h2002m
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_20-Handicraft_v01_PUF", version(12) replace

*	21. HOUSEHOLD HORTICULTURE DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_21-Horticulture_v02",clear

** Preliminary cleaning
	label data "s21 horticulture dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized :  
	drop states h2102n
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_21-Horticulture_v01_PUF", version(12) replace

*	21. HOUSEHOLD HUNTING DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_22-Hunting_v02",clear

** Preliminary cleaning
	label data "s20 hunting dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized :  
	drop states 
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_22-Hunting_v01_PUF", version(12) replace

*	22. HOUSEHOLD LEGAL SERVICES DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_23-LegalServices_v02",clear

** Preliminary cleaning
	label data "s23 legal services dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized :  
	drop states 
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_23-LegalServices_v01_PUF", version(12) replace

*	AGG HOUSEHOLD DATASET 
**	Original	**
use "$TEMP\SPC_PLW_2023_HIES_11-Dwelling_v02", clear

	keep anon_id07 fweight rururb states anon_states anon_hamlet h1101 h1105 h1106 h1117 hhsize
	merge 1:1 anon_id07 using "$TEMP\SPC_PLW_2023_HIES_12-HouseholdAssets_v02", keepusing(h1202__1 h1202__2 h1202__3 h1202__4 h1202__6 h1202__8 h1202__13 h1202__17 h1202__24 h1202__25 h1202__26 h1202__30 h1202__32 h1202__34 h1202__36)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_PLW_2023_HIES_13b-Vehicles_v02", keepusing(h13b2__1 h13b2__2 h13b2__4 h13b2__5)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_PLW_2023_HIES_17-Fisheries_v02", keepusing(h1707 h1707a)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_PLW_2023_HIES_18-Livestock_v02", keepusing(h1802__3 h1802__5 h1802__6)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_PLW_2023_HIES_19-Agriculture_v02", keepusing(h1907__2 h1907__3 h1907__6 h1907__9 h1907__10 h1907__12 h1907__13 h1912__1 h1912__2 h1912__3 h1912__4 h1917__1 h1917__2 h1917__3 h1908__13 h1913__1 h1913__2 h1913__3 h1918__1 h1918__2 h1918__9)
		drop _m

saveold "$TEMP\SPC_PLW_2023_HIES_HouseholdAgg11-22_v03", version(12) replace
	
**	PUF	**	
use "$PUF\SPC_PLW_2023_HIES_11-Dwelling_v01_PUF", clear

	keep anon_id07 fweight rururb anon_states anon_hamlet h1101 h1105 h1106 h1117 hhsize
	merge 1:1 anon_id07 using "$PUF\SPC_PLW_2023_HIES_12-HouseholdAssets_v01_PUF", keepusing(h1202__1 h1202__2 h1202__3 h1202__4 h1202__6 h1202__8 h1202__13 h1202__17 h1202__24 h1202__25 h1202__26 h1202__30 h1202__32 h1202__34 h1202__36)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_PLW_2023_HIES_13b-Vehicles_v01_PUF", keepusing(h13b2__1 h13b2__4 h13b2__5)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_PLW_2023_HIES_17-Fisheries_v01_PUF", keepusing(h1707 h1707a)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_PLW_2023_HIES_18-Livestock_v01_PUF", keepusing(h1802__3 h1802__5)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_PLW_2023_HIES_19-Agriculture_v01_PUF", keepusing(h1907__2 h1907__3 h1907__6 h1907__9 h1907__10 h1907__12 h1907__13 h1912__1 h1912__2 h1912__3 h1912__4 h1917__1 h1917__2 h1917__3 h1908__13 h1913__1 h1913__2 h1913__3 h1918__1 h1918__2 h1918__9)
		drop _m

saveold "$PUF\SPC_PLW_2023_HIES_HouseholdAgg11-23_v01_PUF", version(12) replace

*	30. HOUSEHOLD EXPENDITURE DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_30-ExpenditureAggregate_v02",clear

** Preliminary cleaning
	label data "s30 expenditure aggregates dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop states hamlet buildingGPS__Latitude buildingGPS__Longitude
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_PLW_2023_HIES_30-ExpenditureAggreg_v01_PUF", version(12) replace

*	40. HOUSEHOLD INCOME DATASET	*
use "$TEMP\SPC_PLW_2023_HIES_40-IncomeAggregate_v02",clear

** Preliminary cleaning
	label data "s40 income aggregates dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 4 variables anonymized : main_ISCO main_ISIC sec_ISCO sec_ISIC
	drop states hamlet buildingGPS__Latitude buildingGPS__Longitude 
	recode hhsize (10/16=10)
		label define hhsizel 	10 "10 people or more"
		label values hhsize hhsizel

	drop occ_desc_main ind_desc_main occ_desc_sec ind_desc_sec
	
	*	Aggregating ISIC and ISCO for main activity	
	**	ISIC
	***	2-digit level
	gen ilo_job1_eco_isic4_2digits=. 
	    replace ilo_job1_eco_isic4_2digits=int(main_ISIC/100)
	    replace ilo_job1_eco_isic4_2digits=. if missing(ilo_job1_eco_isic4_2digits)
                lab var ilo_job1_eco_isic4_2digits "Economic activity (ISIC Rev. 4), 2 digits level - main job"

	* 1-digit level
    gen ilo_job1_eco_isic4=.
	    replace ilo_job1_eco_isic4=1 if inrange(ilo_job1_eco_isic4_2digits,1,3)
	    replace ilo_job1_eco_isic4=2 if inrange(ilo_job1_eco_isic4_2digits,5,9)
	    replace ilo_job1_eco_isic4=3 if inrange(ilo_job1_eco_isic4_2digits,10,33)
	    replace ilo_job1_eco_isic4=4 if ilo_job1_eco_isic4_2digits==35
	    replace ilo_job1_eco_isic4=5 if inrange(ilo_job1_eco_isic4_2digits,36,39)
	    replace ilo_job1_eco_isic4=6 if inrange(ilo_job1_eco_isic4_2digits,41,43)
	    replace ilo_job1_eco_isic4=7 if inrange(ilo_job1_eco_isic4_2digits,45,47)
	    replace ilo_job1_eco_isic4=8 if inrange(ilo_job1_eco_isic4_2digits,49,53)
	    replace ilo_job1_eco_isic4=9 if inrange(ilo_job1_eco_isic4_2digits,55,56)
	    replace ilo_job1_eco_isic4=10 if inrange(ilo_job1_eco_isic4_2digits,58,63)
	    replace ilo_job1_eco_isic4=11 if inrange(ilo_job1_eco_isic4_2digits,64,66)
	    replace ilo_job1_eco_isic4=12 if ilo_job1_eco_isic4_2digits==68
	    replace ilo_job1_eco_isic4=13 if inrange(ilo_job1_eco_isic4_2digits,69,75)		
	    replace ilo_job1_eco_isic4=14 if inrange(ilo_job1_eco_isic4_2digits,77,82)
	    replace ilo_job1_eco_isic4=15 if ilo_job1_eco_isic4_2digits==84
        replace ilo_job1_eco_isic4=16 if ilo_job1_eco_isic4_2digits==85
	    replace ilo_job1_eco_isic4=17 if inrange(ilo_job1_eco_isic4_2digits,86,88)
	    replace ilo_job1_eco_isic4=18 if inrange(ilo_job1_eco_isic4_2digits,90,93)
	    replace ilo_job1_eco_isic4=19 if inrange(ilo_job1_eco_isic4_2digits,94,96)
	    replace ilo_job1_eco_isic4=20 if inrange(ilo_job1_eco_isic4_2digits,97,98)
	    replace ilo_job1_eco_isic4=21 if ilo_job1_eco_isic4_2digits==99
	    replace ilo_job1_eco_isic4=22 if ilo_job1_eco_isic4==.
		        lab val ilo_job1_eco_isic4 eco_isic4_1digit
			    lab var ilo_job1_eco_isic4 "Economic activity (ISIC Rev. 4) - main job"
				order ilo_job1_eco_isic4, a(main_ISIC)
		        lab def eco_isic4_1digit 1 "A - Agriculture, forestry and fishing"	2 "B - Mining and quarrying"	3 "C - Manufacturing"	4 "D - Electricity, gas, steam and air conditioning supply" ///
                                         5 "E - Water supply; sewerage, waste management and remediation activities"	6 "F - Construction"	7 "G - Wholesale and retail trade; repair of motor vehicles and motorcycles"	8 "H - Transportation and storage" ///
                                         9 "I - Accommodation and food service activities"	10 "J - Information and communication"	11 "K - Financial and insurance activities"	12 "L - Real estate activities" ///
                                         13 "M - Professional, scientific and technical activities"	14 "N - Administrative and support service activities"	15 "O - Public administration and defence; compulsory social security"	16 "P - Education" ///
                                         17 "Q - Human health and social work activities"	18 "R - Arts, entertainment and recreation"	19 "S - Other service activities"	20 "T - Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use" ///
                                         21 "U - Activities of extraterritorial organizations and bodies"	22 "X - Not elsewhere classified"	
  	  		    lab val ilo_job1_eco_isic4 eco_isic4_1digit
	
	** ISCO
	*** 2-digit level
    gen ilo_job1_ocu_isco08_2digits=. 
	    replace ilo_job1_ocu_isco08_2digits=int(main_ISCO/100)
		replace ilo_job1_ocu_isco08_2digits=. if missing(ilo_job1_ocu_isco08_2digits)
	            lab var ilo_job1_ocu_isco08_2digits "Occupation (ISCO-08), 2 digit level - main job"
				
	***	1-digit level 				
	gen ilo_job1_ocu_isco08=.
	    replace ilo_job1_ocu_isco08=11 if inlist(ilo_job1_ocu_isco08_2digits,.) 			                       	// Not elsewhere classified
		replace ilo_job1_ocu_isco08=int(ilo_job1_ocu_isco08_2digits/10) if ilo_job1_ocu_isco08==.				    // The rest of the occupations
		replace ilo_job1_ocu_isco08=10 if ilo_job1_ocu_isco08==0				                                  	// Armed forces
				lab val ilo_job1_ocu_isco08 ocu_isco08_1digit
				lab var ilo_job1_ocu_isco08 "Occupation (ISCO-08) - main job"
				order ilo_job1_ocu_isco08, a(main_ISCO)
		        lab def ocu_isco08_1digit 1 "1 - Managers"	2 "2 - Professionals"	3 "3 - Technicians and associate professionals"	4 "4 - Clerical support workers" ///
                                          5 "5 - Service and sales workers"	6 "6 - Skilled agricultural, forestry and fishery workers"	7 "7 - Craft and related trades workers"	8 "8 - Plant and machine operators, and assemblers"	///
                                          9 "9 - Elementary occupations"	10 "0 - Armed forces occupations"	11 "X - Not elsewhere classified"		
				lab val ilo_job1_ocu_isco08 ocu_isco08_1digit
	
	*	Aggregating ISIC and ISCO for secondary activity	
	**	ISIC
	***	2-digit level
	gen ilo_job2_eco_isic4_2digits=. 
	    replace ilo_job2_eco_isic4_2digits=int(sec_ISIC/100)
	    replace ilo_job2_eco_isic4_2digits=. if missing(ilo_job2_eco_isic4_2digits)
                lab var ilo_job2_eco_isic4_2digits "Economic activity (ISIC Rev. 4), 2 digits level - second job"

	* 1-digit level
    gen ilo_job2_eco_isic4=.
	    replace ilo_job2_eco_isic4=1 if inrange(ilo_job2_eco_isic4_2digits,1,3)
	    replace ilo_job2_eco_isic4=2 if inrange(ilo_job2_eco_isic4_2digits,5,9)
	    replace ilo_job2_eco_isic4=3 if inrange(ilo_job2_eco_isic4_2digits,10,33)
	    replace ilo_job2_eco_isic4=4 if ilo_job2_eco_isic4_2digits==35
	    replace ilo_job2_eco_isic4=5 if inrange(ilo_job2_eco_isic4_2digits,36,39)
	    replace ilo_job2_eco_isic4=6 if inrange(ilo_job2_eco_isic4_2digits,41,43)
	    replace ilo_job2_eco_isic4=7 if inrange(ilo_job2_eco_isic4_2digits,45,47)
	    replace ilo_job2_eco_isic4=8 if inrange(ilo_job2_eco_isic4_2digits,49,53)
	    replace ilo_job2_eco_isic4=9 if inrange(ilo_job2_eco_isic4_2digits,55,56)
	    replace ilo_job2_eco_isic4=10 if inrange(ilo_job2_eco_isic4_2digits,58,63)
	    replace ilo_job2_eco_isic4=11 if inrange(ilo_job2_eco_isic4_2digits,64,66)
	    replace ilo_job2_eco_isic4=12 if ilo_job2_eco_isic4_2digits==68
	    replace ilo_job2_eco_isic4=13 if inrange(ilo_job2_eco_isic4_2digits,69,75)		
	    replace ilo_job2_eco_isic4=14 if inrange(ilo_job2_eco_isic4_2digits,77,82)
	    replace ilo_job2_eco_isic4=15 if ilo_job2_eco_isic4_2digits==84
        replace ilo_job2_eco_isic4=16 if ilo_job2_eco_isic4_2digits==85
	    replace ilo_job2_eco_isic4=17 if inrange(ilo_job2_eco_isic4_2digits,86,88)
	    replace ilo_job2_eco_isic4=18 if inrange(ilo_job2_eco_isic4_2digits,90,93)
	    replace ilo_job2_eco_isic4=19 if inrange(ilo_job2_eco_isic4_2digits,94,96)
	    replace ilo_job2_eco_isic4=20 if inrange(ilo_job2_eco_isic4_2digits,97,98)
	    replace ilo_job2_eco_isic4=21 if ilo_job2_eco_isic4_2digits==99
	    replace ilo_job2_eco_isic4=22 if ilo_job2_eco_isic4==. 
		        lab val ilo_job2_eco_isic4 eco_isic4_1digit
			    lab var ilo_job2_eco_isic4 "Economic activity (ISIC Rev. 4) - second job"
				order ilo_job2_eco_isic4, a(sec_ISIC)
  	  		    lab val ilo_job2_eco_isic4 eco_isic4_1digit
	
	** ISCO
	*** 2-digit level
    gen ilo_job2_ocu_isco08_2digits=. 
	    replace ilo_job2_ocu_isco08_2digits=int(sec_ISCO/100)
		replace ilo_job2_ocu_isco08_2digits=. if missing(ilo_job2_ocu_isco08_2digits)
	            lab var ilo_job2_ocu_isco08_2digits "Occupation (ISCO-08), 2 digit level - second job"
				
	***	1-digit level 				
	gen ilo_job2_ocu_isco08=.
	    replace ilo_job2_ocu_isco08=11 if inlist(ilo_job2_ocu_isco08_2digits,.)                      	// Not elsewhere classified
		replace ilo_job2_ocu_isco08=int(ilo_job2_ocu_isco08_2digits/10) if ilo_job2_ocu_isco08==.    	// The rest of the occupations
		replace ilo_job2_ocu_isco08=10 if ilo_job2_ocu_isco08==0	                                   	// Armed forces
				lab val ilo_job2_ocu_isco08 ocu_isco08_1digit
				lab var ilo_job2_ocu_isco08 "Occupation (ISCO-08) - second job"
				order ilo_job2_ocu_isco08, a(sec_ISCO)
				lab val ilo_job2_ocu_isco08 ocu_isco08_1digit
		drop ilo_job1_eco_isic4_2digits ilo_job1_ocu_isco08_2digits ilo_job2_eco_isic4_2digits ilo_job2_ocu_isco08_2digits
	drop main_ISCO main_ISIC sec_ISCO sec_ISIC
	
	replace annual_amount=annual_amount*-1 if source==6

saveold "$PUF\SPC_PLW_2023_HIES_40-IncomeAggreg_v01_PUF", version(12) replace

*	-------------------------------------------------------------------------------------------------------------------------------
**	6. Run the post-anonymization datasets through the Statistical Disclosure Control Shiny GUI
*	-------------------------------------------------------------------------------------------------------------------------------
*	0. COVER DATASET	*
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: % ()

*	AGG PERSON DATASET
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: 22% ()
*		Risk of disclosure 3: 23.5% ()

*	AGG HOUSEHOLD DATASET
*		Risk of disclosure 1: 13.2% (124.9)
*		Risk of disclosure 2: % ()
	
*	30. HOUSEHOLD EXPENDITURE DATASET	*
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: % ()

*	40. HOUSEHOLD INCOME DATASET	*
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: % ()






