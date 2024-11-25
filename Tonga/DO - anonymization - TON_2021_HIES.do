*	=========================================	*
*	Olivier Menaouer
*	SDD, SPC
*	14/03/2023
*	Tonga 2021 HIES
*	Anonymization of datasets
*	========================================	*

clear all
version 15
set more off
tempfile temp1 temp2 temp3 temp4 temp5 temp6 temp7 temp8 temp9
global in	"C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Tonga\SPC_TON_2021_HIES_v01_M\Data\Distribute"
global TEMP	"C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Tonga\SPC_TON_2021_HIES_v01_M_v01_A_PUF\Data\Original"
global PUF	"C:\Users\olivierm\OneDrive - SPC\Documents\Work\Data curator\NADA\By country\Tonga\SPC_TON_2021_HIES_v01_M_v01_A_PUF\Data\Distribute"

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
		
*	COVER DATASET	*
use "$in\SPC_TON_2021_HIES_0-Cover_v01",clear

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
	
	** We would usually generate a random Block number as this is an identifier
	preserve
		collapse (first) rururb, by(block)
		gen block_2=runiform()
		sort block_2
		gen anon_block=_n
		la var anon_block "anonymized block number"
		save `temp1', replace
	restore
	merge m:1 block using `temp1'
		assert _merge==3
		drop _merge
	
	** We would usually generate a random Village number as this is an identifier
	preserve
		collapse (first) rururb, by(village)
		gen village_2=runiform()
		sort village_2
		gen anon_village=_n
		la var anon_village "anonymized village number"
		save `temp2', replace
	restore
	merge m:1 village using `temp2'
		assert _merge==3
		drop _merge

	** We would usually generate a random Village number (enumerated area) as this is an identifier
	preserve
		collapse (first) rururb, by(district)
		gen district_2=runiform()
		sort district_2
		gen anon_district=_n
		la var anon_district "anonymized district number"
		save `temp3', replace
	restore
	merge m:1 district using `temp3'
		assert _merge==3
		drop _merge

**	Cleaning the new dataset
	sort anon_id07
	
	order anon_id07, after(interview__key)
	order anon_block, after(block)	
	order anon_village, after(village)
	order anon_district, after(district)
	
	saveold "$TEMP\SPC_TON_2021_HIES_0-Cover_v02", version(12) replace

** Only keep relevant variables
	keep interview__key anon_id07 interview__id fweight island rururb strata anon_district anon_village anon_block district village block hhsize team_id sample round method buildinggps__timestamp interview__status

** We now have all the anonymised variables that we need for the analysis
	** We keep this as a secure (not for public access) master merge file
		
	** Merge this with the other datafiles

		save `temp4', replace

*	1. DEMOGRAPHIC CHARACTERISTICS DATASET	*
use "$in\SPC_TON_2021_HIES_1-DemoCharacter_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key

		save "$TEMP\SPC_TON_2021_HIES_1-DemoCharacter_v02", replace

*	2. PERSON EDUCATION DATASET	*
use "$in\SPC_TON_2021_HIES_2-Education_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_2-Education_v02", replace

*	3. PERSON HEALTH DATASET	*
use "$in\SPC_TON_2021_HIES_3-Health_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_3-Health_v02", replace

*	4. PERSON DISDABILITY DATASET	*
use "$in\SPC_TON_2021_HIES_4-Functionality_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_4-Functionality_v02", replace

*	5. PERSON COMMUNICATION DATASET	*
use "$in\SPC_TON_2021_HIES_5-Communication_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_5-Communication_v02", replace

*	6. PERSON ALCOHOL DATASET	*
use "$in\SPC_TON_2021_HIES_6-Alcohol_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		drop if _merge!=3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_6-Alcohol_v02", replace

*	7. PERSON OTHER INDIV EXP DATASET	*
use "$in\SPC_TON_2021_HIES_7-OtherIndivExp_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_7-OtherIndivExp_v02", replace

*	8. PERSON LABOUR DATASET	*
use "$in\SPC_TON_2021_HIES_8-LabourForce_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_8-LabourForce_v02", replace

*	9. PERSON FISHERIES DATASET	*
use "$in\SPC_TON_2021_HIES_9-Fisheries_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_9-Fisheries_v02", replace

	*	10. PERSON HANDICRAFT DATASET	*
use "$in\SPC_TON_2021_HIES_10-Handicraft_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block)
		assert _merge==3
		drop _merge
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id
	order anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_10-Handicraft_v02", replace

*	11. HOUSEHOLD DWELLING DATASET	*
use "$in\SPC_TON_2021_HIES_11-Dwelling_v01", clear
	la var h1118 "h1128: main water source for cleaning"
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		assert _merge==3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_11-Dwelling_v02", replace

*	12. HOUSEHOLD ASSETS DATASET	*
use "$in\SPC_TON_2021_HIES_12-HouseholdAssets_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		assert _merge==3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_12-HouseholdAssets_v02", replace

*	13a. HOUSEHOLD HOME MAINTENANCE DATASET	*
use "$in\SPC_TON_2021_HIES_13a-HomeMaintenance_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_13a-HomeMaintenance_v02", replace

*	13b. HOUSEHOLD VECHICLES DATASET	*
use "$in\SPC_TON_2021_HIES_13b-Vehicles_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_13b-Vehicles_v02", replace

*	13c. HOUSEHOLD INTERNAT TRAVEL DATASET	*
use "$in\SPC_TON_2021_HIES_13c-InternationalTravel_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_13c-InternationalTravel_v02", replace

*	13d. HOUSEHOLD DOMESTIC TRAVEL DATASET	*
use "$in\SPC_TON_2021_HIES_13d-DomesticTravel_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_13d-DomesticTravel_v02", replace

*	13e. HOUSEHOLD SERVICES DATASET	*
use "$in\SPC_TON_2021_HIES_13e-HouseholdServices_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_13e-HouseholdServices_v02", replace

*	13f. HOUSEHOLD FINANCIAL SUPPORT DATASET	*
use "$in\SPC_TON_2021_HIES_13f-FinancialSupport_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_13f-FinancialSupport_v02", replace

*	13g. HOUSEHOLD OTHER HH EXP DATASET	*
use "$in\SPC_TON_2021_HIES_13g-OtherHhldExpenditure_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_13g-OtherHhldExpenditure_v02", replace

*	14. HOUSEHOLD CEREMONIES DATASET	*
use "$in\SPC_TON_2021_HIES_14-Ceremonies_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_14-Ceremonies_v02", replace

*	15. HOUSEHOLD REMITTANCES DATASET	*
use "$in\SPC_TON_2021_HIES_15-Remittances_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_15-Remittances_v02", replace

*	16. HOUSEHOLD FIES DATASET	*
use "$in\SPC_TON_2021_HIES_16-FoodInsecurity_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_16-FoodInsecurity_v02", replace

*	18. HOUSEHOLD LIVESTOCK AQUAC DATASET	*
use "$in\SPC_TON_2021_HIES_18-LivestockAqua_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_18-LivestockAqua_v02", replace

*	19a. HOUSEHOLD AGRIC PARCEL DATASET	*
use "$in\SPC_TON_2021_HIES_19a-AgricultureParcel_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_19a-AgricultureParcel_v02", replace

*	19b. HOUSEHOLD AGRIC VEGETABLES DATASET	*
use "$in\SPC_TON_2021_HIES_19b-AgricultureVegetables_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_19b-AgricultureVegetables_v02", replace

*	19c. HOUSEHOLD AGRIC ROOT CROP DATASET	*
use "$in\SPC_TON_2021_HIES_19c-AgricultureRootcrops_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_19c-AgricultureRootcrops_v02", replace

*	19d. HOUSEHOLD AGRIC OTHER PLANTS DATASET	*
use "$in\SPC_TON_2021_HIES_19d-AgricultureOtherPlants_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_19d-AgricultureOtherPlants_v02", replace

*	19e. HOUSEHOLD AGRIC FRUIT DATASET	*
use "$in\SPC_TON_2021_HIES_19e-AgricultureFruits_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_19e-AgricultureFruits_v02", replace

*	20. HOUSEHOLD LEGAL SERVICES DATASET	*
use "$in\SPC_TON_2021_HIES_20-LegalServices_v01", clear
	merge 1:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		drop if _merge!=3
		drop _merge
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key interview__id

		save "$TEMP\SPC_TON_2021_HIES_20-LegalServices_v02", replace		
		
*	30. HOUSEHOLD EXPENDITURE DATASET	*
use "$in\SPC_TON_2021_HIES_30-ExpenditureAggregate_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		assert _merge==3
		drop _merge
	bysort interview__key : gen temp=_n
		encode interview__key, gen(temp2)
		egen exp_id=concat(temp2 temp), punct(.)								//	one single identifier variable is required in sdcMicro so concatenate of household ID + transaction nb
		drop temp temp2
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_30-ExpenditureAggregate_v02", replace

*	40. HOUSEHOLD INCOME DATASET	*
use "$in\SPC_TON_2021_HIES_40-IncomeAggregate_v01", clear
	merge m:1 interview__key using `temp4', keepusing(anon_id07 fweight island rururb strata anon_district anon_village anon_block hhsize)
		assert _merge==3
		drop _merge
	bysort interview__key : gen temp=_n
		encode interview__key, gen(temp2)
		egen exp_id=concat(temp2 temp), punct(.)								//	one single identifier variable is required in sdcMicro so concatenate of household ID + transaction nb
		drop temp temp2
	order anon_id07 fweight rururb island strata anon_district anon_village anon_block, first
	sort anon_id07
	drop interview__key 

		save "$TEMP\SPC_TON_2021_HIES_40-IncomeAggregate_v02", replace

use `temp4', clear
	drop interview__key interview__id district village block
	save "$PUF\SPC_TON_2021_HIES_0-Cover_v01_PUF", replace

*	-------------------------------------------------------------------------------------------------------------------------------
**	3. Identify "keys" in the dataset that may be used to identify individuals / households
*	-------------------------------------------------------------------------------------------------------------------------------
*	0. COVER DATASET	*
*		Scenario 1: strata block hhsize
*		Scenario 2: 

*	XX. PERSON RECORD DATASET		*
*		Scenario 1: sex age p211 p301 p303 p602e__1 p812 p814
*		Scenario 2: 
	
*	XX. HOUSEHOLD RECORD DATASET			*
*		Scenario 1: strata h1102 h1105 h1202__3 h13b2__1 h13c3__1 h1802__1 h1907__12 h1912__5
*		Scenario 2:  strata hhsize h1105 h1106 h1122h h1202__1 h13b2__1 h1802__3 h1922__5
		
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
*		Risk of disclosure 1: 16.94% (360.87 re-identifications in the population)
*		Risk of disclosure 2: % ()

*	AGG PERSON DATASET
*		Risk of disclosure 1: 27.47% (3,037.93 re-identifications in the population)
*		Risk of disclosure 2: % ()

*	AGG HOUSEHOLD DATASET
*		Risk of disclosure 1: 6.4% (136.4 re-identifications in the population)
*		Risk of disclosure 2: 31.09% (662.3 re-identifications in the population)
	
*	30. HOUSEHOLD EXPENDITURE DATASET	*
*		Risk of disclosure 1: 2.55% (4,579.37 re-identifications in the population)
*		Risk of disclosure 2: % ()

*	40. HOUSEHOLD INCOME DATASET	*
*		Risk of disclosure 1: 3.41% (1,858.64 re-identifications in the population)
*		Risk of disclosure 2: % ()

*	-------------------------------------------------------------------------------------------------------------------------------
**	5. Anonymise the "keys" by top/bottom coding, aggregating, other;
*	-------------------------------------------------------------------------------------------------------------------------------
*	COVER DATASET	*
** Preliminary cleaning
	label data "s0 cover dataset"

** Recoding variables and recode the "0" as missing (.)
* 1 variable anonymized : hhsize
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel
	
saveold "$PUF\SPC_TON_2021_HIES_0-Cover_v01_PUF", version(12) replace
			
*	1. PERSON DEMOG CHARACT DATASET
use "$TEMP\SPC_TON_2021_HIES_1-DemoCharacter_v02",clear

** Preliminary cleaning
	label data "s1 demographic characteristics dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 4 variables anonymized : age p107 p108 p112
	drop p108n p112n p114n
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

	drop p107	// too risky
	recode p108 (4=8)
	recode p114 (19 20=98)

saveold "$PUF\SPC_TON_2021_HIES_1-DemoCharacter_v01_PUF", version(12) replace

*	2. PERSON EDUCATION DATASET	*
use "$TEMP\SPC_TON_2021_HIES_2-Education_v02",clear

** Preliminary cleaning
	label data "s2 education dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 3 variables anonymized : p206 p209a p211
	drop p202n p203n p204n p206n p207n p210n
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age
	
	recode p206 (4 5=9)
	recode p209a (9=11)
		la def p209a	11 "Transportation unavailable or closest school full", modify
			la val p209a p209a
	recode p211 (64 66=64) (71=81)
		la def p211 64 "Master's degree or PhD", modify
			la val p211 p211

saveold "$PUF\SPC_TON_2021_HIES_2-Education_v01_PUF", version(12) replace

*	3. PERSON HEALTH DATASET	*
use "$TEMP\SPC_TON_2021_HIES_3-Health_v02",clear

** Preliminary cleaning
	label data "s3 health dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 12 variables anonymized : p301 p302 p303 p306__1 p306__2 p306__3 p306__4 p306__5 p306__6 p306__8 p306__9 p306__13
	drop p304an p317n__1 p318n__1 p315n__2 p317n__2 p318n__2 p315n__3 p317n__3 p318n__3 p315n__4 p317n__4 p318n__4 p315n__5 p317n__5 p318n__5 p315n__6 p317n__6 p318n__6 p315n__7 p317n__7 p318n__7 p315n__8 p317n__8 p318n__8 p315n__9 p317n__9 p318n__9 p308n p320n
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age
	
	recode p301 (90.5/99.99=1) (100/104.99=2) (105/109.99=3) (110/114.99=4) (115/119.99=5) (120/124.99=6) (125/129.99=7) (130/134.99=8) (135/139.99=9) (140/144.99=10) (145/149.99=11) (150/154.99=12) (155/159.99=13) (160/164.999=14) (165/169.99=15) (170/174.99=16) ///
										(175/179.99=17) (180/184.99=18) (185/189.99=19) (190/194.99=20) (195/206=21), gen(anon_p301)
		label define p301l				1 "Less than 1m" 2 "100-104cm" 3 "105-109cm" 4 "110-114cm" 5 "115-119cm" 6 "120-124cm" 7 "125-129cm" 8 "130-134cm" 9 "135-139cm" 10 "140-144cm" 11 "145-149cm" 12 "150-154cm" 13 "155-159cm" 14 "160-164cm" 15 "165-169cm" 16 "170-174cm" 17 "175-179cm" 18 "180-184cm" 19 "185-189cm" 20 "190-194cm" 21 "195cm or more"
		label values anon_p301 p301l
			order anon_p301, a(p301)
			la var anon_p301 "recode of p301: height in meters and cm"
			drop p301
	recode p302 (36/49.99=49) (173/225=173), gen(anon1_p302)
			la def	p302l	49 "Less than 50cm" 173 "173cm or more"
		gen anon_p302=round(anon1_p302,0.5)											// Centimeters anonymized (rounded) to the nearest 0.5
			la val anon_p302 p302l
			order anon_p302,a(p302)
			la var anon_p302 "recode of p302: waist in cm - rounded 0.5"
			drop p302 anon1_p302
	recode p303 (13/19.99=19) (150.2/227=151), gen(anon1_p303)
		la def p303l					19 "Less than 20kg" 151 "More than 150kg"
		la val anon1_p303 p303l
		gen anon_p303=round(anon1_p303,0.5)											// Kilograms anonymized (rounded) to the nearest 0.5
			la val anon_p303 p303l
				order anon_p303, a(p303)
				la var anon_p303 "recode of p303: weight in kgs - rounded 0.5"
				drop p303 anon1_p303
	recode p306__1 (2/max=2)
		la def p306__1l 2 "2 times or more"
		la val p306__1 p306__1l
	recode p306__2 (4/max=4)
		la def p306__2l 4 "4 times or more"
		la val p306__2 p306__2l
	recode p306__3 (4/max=4)
		la def p306__3l 4 "4 times or more"
		la val p306__3 p306__3l
	recode p306__4 (3/max=3)
		la def p306__4l 3 "3 times or more"
		la val p306__4 p306__4l
	recode p306__5 (2/max=2)
		la def p306__5l 2 "2 times or more"
		la val p306__5 p306__5l
	recode p306__6 (2/max=2)
		la def p306__6l 2 "2 times or more"
		la val p306__6 p306__6l
	recode p306__8 (4/max=4)
		la def p306__8l 4 "4 times or more"
		la val p306__8 p306__8l
	recode p306__9 (4/max=4)
		la def p306__9l 4 "4 times or more"
		la val p306__9 p306__9l
	recode p306__13 (3/max=3)
		la def p306__13l 3 "3 times or more"
		la val p306__13 p306__13l

saveold "$PUF\SPC_TON_2021_HIES_3-Health_v01_PUF", version(12) replace

*	4. PERSON DISABILITY DATASET	*
use "$TEMP\SPC_TON_2021_HIES_4-Functionality_v02",clear

** Preliminary cleaning
	label data "s4 functionality dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_TON_2021_HIES_4-Functionality_v01_PUF", version(12) replace

*	5. PERSON COMMUNICATION DATASET	*
use "$TEMP\SPC_TON_2021_HIES_5-Communication_v02",clear

** Preliminary cleaning
	label data "s5 communication dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop p502n
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_TON_2021_HIES_5-Communication_v01_PUF", version(12) replace

*	6. PERSON ALCOHOL DATASET	*
use "$TEMP\SPC_TON_2021_HIES_6-Alcohol_v02",clear

** Preliminary cleaning
	label data "s6 alcohol dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 14 variables anonymized : p602e__1 p602e__2 p602e__3 p602e__5 p602e__6 p602e__8 p602m__5 p602m__6 p602q__1 p602q__2 p602q__3 p602q__5 p602q__6 p602q__8
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

recode p602e__1 (100/4500=100)
	la def p602e__1l	100 "100 or more"
	la val p602e__1 p602e__1l
recode p602e__2 (100/750=100)
	la def p602e__2l	100 "100 or more"
	la val p602e__2 p602e__2l
recode p602e__3 (500/750=500)
	la def p602e__3l	500 "500 or more"
	la val p602e__3 p602e__3l
recode p602e__5 (200/800=200)
	la def p602e__5l	200 "200 or more"
	la val p602e__5 p602e__5l
recode p602e__6 (101/500=101)
	la def p602e__6l	101 "More than 100"
	la val p602e__6 p602e__6l
recode p602e__8 (150/455=150)
	la def p602e__8l	150 "150 or more"
	la val p602e__8 p602e__8l
recode p602m__5 (20/max=20)
	la def p602m__5l	20 "20 or more"
	la val p602m__5 p602m__5l
recode p602m__6 (50/max=50)
	la def p602m__6l	50 "50 or more"
	la val p602m__6 p602m__6l
recode p602q__1 (750/max=700)
	la def p602q__1l	700 "700 or more"
	la val p602q__1 p602q__1l
recode p602q__2 (10/max=10)
	la def p602q__2l	10 "10 or more"
	la val p602q__2 p602q__2l
recode p602q__3 (101/max=101)
	la def p602q__3l	101 "More than 100"
	la val p602q__3 p602q__3l
recode p602q__5 (500/max=500)
	la def p602q__5l	500 "500 or more"
	la val p602q__5 p602q__5l
recode p602q__6 (50/max=50)
	la def p602q__6l	50 "50 or more"
	la val p602q__6 p602q__6l
recode p602q__8 (25/max=25)
	la def p602q__8l	25 "25 or more"
	la val p602q__8 p602q__8l

saveold "$PUF\SPC_TON_2021_HIES_6-Alcohol_v01_PUF", version(12) replace

*	7. PERSON OTHER INDIV EXP DATASET	*
use "$TEMP\SPC_TON_2021_HIES_7-OtherIndivExp_v02",clear

** Preliminary cleaning
	label data "s7 other individual expenditure dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age
	
saveold "$PUF\SPC_TON_2021_HIES_7-OtherIndivExp_v01_PUF", version(12) replace

*	8. PERSON LABOUR DATASET	*
use "$TEMP\SPC_TON_2021_HIES_8-LabourForce_v02",clear

** Preliminary cleaning
	label data "s8 labour dataset - individual"
	order team_id interviewer_id sample round method int_avail int_reason interview__status, last

** Recoding variables and recode the "0" as missing (.)
* 15 variables anonymized : p802 p804 p810 p811 p812 p813 p814 p844 p845 p846 p847 p852 p853 p854 p858b
	drop hhld_id district village block interview__id
	drop p801n p802n1 p802n2 p802an p810n p815n p825n p830n p836n p843n p850n p858c
	drop ilo_key ilo_hh ilo_time ilo_geo ilo_sex ilo_age ilo_age_5yrbands ilo_age_10yrbands ilo_age_aggregate ilo_edu_isced11 ilo_edu_aggregate ilo_edu_attendance ilo_placeofbirth ilo_relationship_details ilo_relationship_aggregate ilo_mrts_details ilo_mrts_aggregate ilo_dsb_details ilo_dsb_aggregate

	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

	recode p802 (6 8 9=9) (12 13 17 18=21) 
		la def p802						9 "Other livestock", modify
		la val p802 p802
	recode p804 (1/14=1) (15/29=2) (30/34=3) (35/39=4) (40/48=5) (49/max=6)
		la def p804lab	0 "No hours actually worked" 1 "01-14 hours" 2 "15-29 hours" 3 "30-34 hours" 4 "35-39 hours" 5 "40-48 hours" 6 "49+ hours"
		la val p804 p804lab
	recode p810 (12 3=3) (1 13=13)
	drop p811
	drop p812 
	drop p813
	drop p814
	drop p844
	drop p845
	drop p846
	drop p847
	recode p852 (1/14=1) (15/29=2) (30/34=3) (35/39=4) (40/48=5) (49/max=6)
		la def p852lab	0 "No hours actually worked" 1 "01-14 hours" 2 "15-29 hours" 3 "30-34 hours" 4 "35-39 hours" 5 "40-48 hours" 6 "49+ hours"
		la val p852 p852lab
	recode p853 (1/14=1) (15/29=2) (30/34=3) (35/39=4) (40/48=5) (49/max=6)
		la def p853lab	0 "No hours actually worked" 1 "01-14 hours" 2 "15-29 hours" 3 "30-34 hours" 4 "35-39 hours" 5 "40-48 hours" 6 "49+ hours"
		la val p853 p853lab
	recode p854 (1/14=1) (15/29=2) (30/34=3) (35/39=4) (40/48=5) (49/max=6)
		la def p854lab	0 "No hours actually worked" 1 "01-14 hours" 2 "15-29 hours" 3 "30-34 hours" 4 "35-39 hours" 5 "40-48 hours" 6 "49+ hours"
		la val p854 p854lab
	recode p858b (13 14=14)

saveold "$PUF\SPC_TON_2021_HIES_8-Labour_v01_PUF", version(12) replace

*	9. PERSON FISHERIES DATASET	*
use "$TEMP\SPC_TON_2021_HIES_9-Fisheries_v02",clear

** Preliminary cleaning
	label data "s9 fisheries dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 14 variables anonymized : p902 p907__1 p907__2 p907a__2 p907__2 p907__4 p907__5 p907a__5 p907__6 p907a__6 p907__7 p907__8 p907__9 p907__11
	drop p906n__1 p906an__1 p906n__2 p906an__2 p906n__3 p906an__3 p906n__4 p906an__4 p906n__5 p906an__5 p906n__6 p906an__6 p906n__7 p906an__7 p906n__8 p906an__8 p906n__9 p906an__9 p906n__11 p906an__11 p903n p922n1 p922n2 p922n3 p921nr
	
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

	recode p902 (1/14=1) (15/29=2) (30/34=3) (35/39=4) (40/48=5) (49/max=6)
		la def p902lab	0 "No hours spent fishing" 1 "01-14 hours" 2 "15-29 hours" 3 "30-34 hours" 4 "35-39 hours" 5 "40-48 hours" 6 "49+ hours"
		la val p902 p902lab
	recode p907__1 (6/max=6)
		la def p907__1	6 "6 or more"
		la val p907__1 p907__1
	recode p907__2 (12/max=12)
		la def p907__2	12 "12 or more"
		la val p907__2 p907__2
	recode p907a__2 (3/max=3)
		la def p907a__2	3 "3 or more"
		la val p907a__2 p907a__2
	recode p907__2 (35=.)
	recode p907__4 (5/max=5)
		la def p907__4 5 "5 or more"
		la val p907__4 p907__4
	recode p907__5 (30/max=30)
		la def p907__5 30 "30 or more"
		la val p907__5 p907__5
	recode p907a__5 (5/max=5)
		la def p907a__5 5 "5 or more"
		la val p907a__5 p907a__5
	recode p907__6 (20/max=20)
		la def p907__6 20 "20 or more"
		la val p907__6 p907__6
	recode p907a__6 (6/max=6)
		la def p907a__6 6 "6 or more"
		la val p907a__6 p907a__6
	recode p907__7 (12/max=12)
		la def p907__7 12 "12 or more"
		la val p907__7 p907__7
	recode p907__8 (6/max=6)
		la def p907__8 6 "6 or more"
		la val p907__8 p907__8
	recode p907__9 (16/max=16)
		la def p907__9 16 "16 or more"
		la val p907__9 p907__9
	recode p907__11 (16/max=16)
		la def p907__11 16 "16 or more"
		la val p907__11 p907__11

saveold "$PUF\SPC_TON_2021_HIES_9-Fisheries_v01_PUF", version(12) replace

*	10. PERSON HANDICRAFT DATASET	*
use "$TEMP\SPC_TON_2021_HIES_10-Handicraft_v02",clear

** Preliminary cleaning
	label data "s10 handicraft dataset - individual"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	recode age (0/4=1) (5/9=2) (10/14=3) (15/19=4) (20/24=5) (25/29=6) (30/34=7) (35/39=8) (40/44=9) (45/49=10) (50/54=11) (55/59=12) (60/64=13) (65/69=14) (70/74=15) (75/116=16), gen(age_grp5)
		la def ageyl					1 "0-4 years" 2 "5-9 years" 3 "10-14 years" 4 "15-19 years" 5 "20-24 years" 6 "25-29 years" 7 "30-34 years" 8 "35-39 years" 9 "40-44 years" 10 "45-49 years" 11 "50-54 years" ///
										12 "55-59 years" 13 "60-64 years" 14 "65-69 years" 15 "70-74 years" 16 "75 years and older"
		la val age_grp5 ageyl
			order age_grp5, after(age)
			la var age_grp5 "recode of age: age group 5"
	drop age

saveold "$PUF\SPC_TON_2021_HIES_10-Handicraft_v01_PUF", version(12) replace

*	AGG PERSON DATASET
**	Original	**
use "$TEMP\SPC_TON_2021_HIES_1-DemoCharacter_v02", clear

	keep anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block sex age p114 
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_TON_2021_HIES_2-Education_v02",				keepusing(p206 p209a p211)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_TON_2021_HIES_3-Health_v02",					keepusing(p301 p302 p303 p306__1 p306__2 p306__3 p306__4 p306__5 p306__6 p306__8 p306__9 p306__13)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_TON_2021_HIES_6-Alcohol_v02",					keepusing(p602e__1 p602e__2 p602e__3 p602e__5 p602e__6 p602e__8 p602m__5 p602m__6 p602q__1 p602q__2 p602q__3 p602q__5 p602q__6 p602q__8)
		drop _m		
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_TON_2021_HIES_8-LabourForce_v02",				keepusing(p802 p804 p810 p812 p814 p852 p853 p854 p858b)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$TEMP\SPC_TON_2021_HIES_9-Fisheries_v02",				keepusing(p902 p907__1 p907__2 p907a__2 p907__2 p907__4 p907__5 p907a__5 p907__6 p907a__6 p907__7 p907__8 p907__9 p907__11)
		drop _m
	
	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id

saveold "$TEMP\SPC_TON_2021_HIES_PersonAgg1-10_v02", version(12) replace
	
**	PUF	**	
use "$PUF\SPC_TON_2021_HIES_1-DemoCharacter_v01_PUF", clear

	keep anon_id07 hm_basic__id fweight rururb island strata anon_district anon_village anon_block sex age p114 
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_TON_2021_HIES_2-Education_v01_PUF",			keepusing(p206 p209a p211)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_TON_2021_HIES_3-Health_v01_PUF",				keepusing(anon_p301 anon_p302 anon_p303 p306__1 p306__2 p306__3 p306__4 p306__5 p306__6 p306__8 p306__9 p306__13)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_TON_2021_HIES_6-Alcohol_v01_PUF",				keepusing(p602e__1 p602e__2 p602e__3 p602e__5 p602e__6 p602e__8 p602m__5 p602m__6 p602q__1 p602q__2 p602q__3 p602q__5 p602q__6 p602q__8)
		drop _m		
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_TON_2021_HIES_8-Labour_v01_PUF",				keepusing(p802 p804 p810 ilo_job1_ocu_isco08_2digits ilo_job1_eco_isic4_2digits p852 p853 p854 p858b)
		drop _m
	merge 1:1 anon_id07 hm_basic__id using "$PUF\SPC_TON_2021_HIES_9-Fisheries_v01_PUF",			keepusing(p902)
		drop _m

	egen pers_id =concat(anon_id07 hm_basic__id), punct(.)						//	one single identifier variable is required in sdcMicro so concatenate of household ID + person ID
		encode pers_id, gen(pers_id2)
		drop pers_id

saveold "$PUF\SPC_TON_2021_HIES_PersonAgg1-10_v01_PUF", version(12) replace
	
*	11. HOUSEHOLD DWELLING DATASET	*
use "$TEMP\SPC_TON_2021_HIES_11-Dwelling_v02",clear

** Preliminary cleaning
	label data "s11 dwelling characteristics dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 6 variables anonymized : h1102 h1103 h1104 h1105 h1106 h1122h
	drop h1101n h1102n h1103n h1104n h1107n h1112n__1 h1112n__2 h1112n__3 h1112n__4 h1112n__5 h1112n__6 h1112n__7 h1112n__8 h1112n__9 h1108n h1119n h1121n
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h1102 (3 4=5)
		la def h1102 5 "Other", modify
		la val h1102 h1102
	recode h1103 (4 5=5)
	recode h1104 (2 4=5)
	recode h1105 (10/max=10)
		la def h1105l	10 "10 or more"
		la val h1105 h1105l
	recode h1106 (1921/1974=1) (1975/1989=2) (1990/1999=3) (2000/2009=4) (2010/2014=5) (2015/2021=6) (0=9999)
		la def h1106l	1 "Before 1975" 2 "1975-1989" 3 "1990-1999" 4 "2000-2009" 5 "2010-2014" 6 "2015-2021" 9999 "Unknown"
		la val h1106 h1106l
	recode h1122h (5/20=5)
		la def h1122hl	5 "5 or more"
		la val h1122h h1122hl

saveold "$PUF\SPC_TON_2021_HIES_11-Dwelling_v01_PUF", version(12) replace

*	12. HOUSEHOLD ASSETS DATASET	*
use "$TEMP\SPC_TON_2021_HIES_12-HouseholdAssets_v02",clear

** Preliminary cleaning
	label data "s12 household assets dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 14 variables anonymized : h1202__1 h1202__2 h1202__3 h1202__4 h1202__6 h1202__8 h1202__13 h1202__24 h1202__25 h1202__26 h1202__30 h1202__32 h1202__34 h1202__36
	drop h1201n1 h1201n2 h1201n3 h1201n4
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h1202__1 (10/14=10)
		la def h1202__1			10 "10 or more"
		la val h1202__1 h1202__1
	recode h1202__2 (11/20=11)
		la def h1202__2			11 "11 or more"
		la val h1202__2 h1202__2
	recode h1202__3 (10/16=10)
		la def h1202__3			10 "10 or more"
		la val h1202__3 h1202__3
	recode h1202__4 (6/14=6)
		la def h1202__4			6 "6 or more"
		la val h1202__4 h1202__4
	recode h1202__6 (4/10=4)
		la def h1202__6		4 "4 or more"
		la val h1202__6 h1202__6
	recode h1202__8 (3/10=3)
		la def h1202__8		3 "3 or more"
		la val h1202__8 h1202__8
	recode h1202__13 (2/10=2)
		la def h1202__13		2 "2 or more"
		la val h1202__13 h1202__13
	recode h1202__24 (5 6=5)
		la def h1202__24		5 "5 or more"
		la val h1202__24 h1202__24
	recode h1202__25 (6/8=6)
		la def h1202__25		6 "6 or more"
		la val h1202__25 h1202__25
	recode h1202__26 (3/7=3)
		la def h1202__26		3 "3 or more"
		la val h1202__26 h1202__26
	recode h1202__30 (3/5=3)
		la def h1202__30		3 "3 or more"
		la val h1202__30 h1202__30
	recode h1202__32 (4/7=4)
		la def h1202__32		4 "4 or more"
		la val h1202__32 h1202__32
	recode h1202__34 (1 6=1)
		la def h1202__34		1 "At least 1"
		la val h1202__34 h1202__34
	recode h1202__36 (3/16=3)
		la def h1202__36		3 "3 or more"
		la val h1202__36 h1202__36

saveold "$PUF\SPC_TON_2021_HIES_12-HouseholdAssets_v01_PUF", version(12) replace

*	13a. HOUSEHOLD HOME MAINTENANCE DATASET	*
use "$TEMP\SPC_TON_2021_HIES_13a-HomeMaintenance_v02",clear

** Preliminary cleaning
	label data "s13a home maintenance dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop h13a1n
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_TON_2021_HIES_13a-HomeMaintenance_v01_PUF", version(12) replace

*	13b. HOUSEHOLD VECHICLES DATASET	*

use "$TEMP\SPC_TON_2021_HIES_13b-Vehicles_v02",clear

** Preliminary cleaning
	label data "s13b vehicles dataset - household"
	
** Recoding variables and recode the "0" as missing (.)
* 3 variable anonymized : h13b2__1 h13b2__2 h13b2__4
	drop h13b1n
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel
 
	recode h13b2__1 (4/8=4)
		la def h13b2__1l			4 "4 or more"
		la val h13b2__1 h13b2__1l
	recode h13b2__2 (2/5=2)
		la def h13b2__2l			2 "2 or more"
		la val h13b2__2 h13b2__2l
	recode h13b2__4 (3/10=3)
		la def h13b2__4l			3 "3 or more"
		la val h13b2__4 h13b2__4l

saveold "$PUF\SPC_TON_2021_HIES_13b-Vehicles_v01_PUF", version(12) replace

*	13c. HOUSEHOLD INTERNAT TRAVEL DATASET	*
use "$TEMP\SPC_TON_2021_HIES_13c-InternationalTravel_v02",clear

** Preliminary cleaning
	label data "s13c international travel dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 2 variables anonymized : h13c3__1 h13c3__2
	drop h13c3n__1 h13c4n__1 h13c3n__2 h13c4n__2
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h13c3__1 (3 4 6=7)
	recode h13c3__2 (1 2=.)
	
saveold "$PUF\SPC_TON_2021_HIES_13c-InternationalTravel_v01_PUF", version(12) replace

*	13d. HOUSEHOLD DOMESTIC TRAVEL DATASET	*
use "$TEMP\SPC_TON_2021_HIES_13d-DomesticTravel_v02",clear

** Preliminary cleaning
	label data "s13d domestic travel dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 2 variable anonymized : h13d3__1 h13d3__2
	drop h13d4n__1 h13d4n__2 h13d4n__3 h13d4n__4 h13d4n__5 h13d4n__6 h13d4n__7 h13d4n__8 h13d4n__9 h13d4n__10 h13d4n__11 h13d4n__12
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel
	
	recode h13d3__1 (8 9=8)
		la def h13d3__1l	1 "Tongatapu" 2 "Tongatapu islands" 3 "Vava'u main island" 4 "Vava'u islands" 5 "Ha'apai main island" 6 "Ha'apai islands" 7 "'Eua" 8 "Ongo Niua"
		la val h13d3__1 h13d3__1l
	recode h13d3__2 (8 9=8)
		la def h13d3__2l	1 "Tongatapu" 2 "Tongatapu islands" 3 "Vava'u main island" 4 "Vava'u islands" 5 "Ha'apai main island" 6 "Ha'apai islands" 7 "'Eua" 8 "Ongo Niua"
		la val h13d3__2 h13d3__2l

saveold "$PUF\SPC_TON_2021_HIES_13d-DomesticTravel_v01_PUF", version(12) replace

*	13e. HOUSEHOLD SERVICES DATASET	*
use "$TEMP\SPC_TON_2021_HIES_13e-HouseholdServices_v02",clear

** Preliminary cleaning
	label data "s13e household services dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop h13e1n1 h13e1n2 h13e1n3
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_TON_2021_HIES_13e-HouseholdServices_v01_PUF", version(12) replace

*	13f. HOUSEHOLD FINANCIAL SUPPORT DATASET	*
use "$TEMP\SPC_TON_2021_HIES_13f-FinancialSupport_v02",clear

** Preliminary cleaning
	label data "s13f financial support dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop h13f1n
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel
	
saveold "$PUF\SPC_TON_2021_HIES_13f-FinancialSupport_v01_PUF", version(12) replace

*	13g. HOUSEHOLD OTHER HH EXP DATASET	*
use "$TEMP\SPC_TON_2021_HIES_13g-OtherHhldExpenditure_v02",clear

** Preliminary cleaning
	label data "s13g other household expenditure dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop h13g1n1 h13g1n2
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_TON_2021_HIES_13g-OtherHhldExp_v01_PUF", version(12) replace

*	14. HOUSEHOLD CEREMONIES DATASET	*
use "$TEMP\SPC_TON_2021_HIES_14-Ceremonies_v02",clear

** Preliminary cleaning
	label data "s14 ceremonies dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_TON_2021_HIES_14-Ceremonies_v01_PUF", version(12) replace

*	15. HOUSEHOLD REMITTANCES DATASET	*
use "$TEMP\SPC_TON_2021_HIES_15-Remittances_v02",clear

** Preliminary cleaning
	label data "s15 remittances dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	drop h1503ro__1 h1503ro__2 h1503ro__3 h1503ro__4 h1503ro__5 h1503ro__6 h1503ro__7 h1503ro__8 h1503ro__9 h1503ro__10 h1503ro__11 h1503ro__12 h1503ro__13 h1503ro__14 h1503ro__15
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_TON_2021_HIES_15-Remittances_v01_PUF", version(12) replace

*	16. HOUSEHOLD FIES DATASET	*
use "$TEMP\SPC_TON_2021_HIES_16-FoodInsecurity_v02",clear

** Preliminary cleaning
	label data "s16 FIES dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_TON_2021_HIES_16-FIES_v01_PUF", version(12) replace

*	18. HOUSEHOLD LIVESTOCK AQUAC DATASET	*
use "$TEMP\SPC_TON_2021_HIES_18-LivestockAqua_v02",clear

** Preliminary cleaning
	label data "s18 livestock aquaculture dataset - household"
	drop h1803__4_1 h1803__5_1 h1803__6_1 h1803__7_1 h1803__8_1
	
** Recoding variables and recode the "0" as missing (.)
* 8 variables anonymized : h1802__1 h1802__2 h1802__3 h1802__4 h1802__5 h1802__6 h1802__7 h1805__3
	drop h1801n1
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h1802__1 (20/85=20)
		la def h1802__1l			20 "20 or more"
		la val h1802__1 h1802__1l
	recode h1802__2 (5/10=5)
		la def h1802__2l			5 "5 or more"
		la val h1802__2 h1802__2l
	recode h1802__3 (50/109=50)
		la def h1802__3l			50 "50 or more"
		la val h1802__3 h1802__3l
	recode h1802__4 (5/32=5)
		la def h1802__4l			5 "5 or more"
		la val h1802__4 h1802__4l
	recode h1802__5 (4/20=4)
		la def h1802__5l			4 "4 or more"
		la val h1802__5 h1802__5l
	recode h1802__6 (60/150=60)
		la def h1802__6l			60 "60 or more"
		la val h1802__6 h1802__6l
	recode h1802__7 (9/52=9)
		la def h1802__7l			9 "9 or more"
		la val h1802__7 h1802__7l
	recode h1805__3 (100/600=100)
		la def h1805__3l			100 "100 or more"
		la val h1805__3 h1805__3l

saveold "$PUF\SPC_TON_2021_HIES_18-LivestockAqua_v01_PUF", version(12) replace

*	19a. HOUSEHOLD AGRIC PARCEL DATASET	*
use "$TEMP\SPC_TON_2021_HIES_19a-AgricultureParcel_v02",clear

** Preliminary cleaning
	label data "s19a agriculture parcel dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized :
	drop h1906an__0 h1911an__0 h1916an__0 h1921an__0 h1906an__1 h1911an__1 h1916an__1 h1921an__1 h1906an__2 h1911an__2 h1916an__2 h1921an__2 h1906an__3 h1911an__3 h1916an__3 h1921an__3 h1906an__4 h1911an__4 h1916an__4 h1921an__4 h1906an__5 h1911an__5 h1916an__5 h1921an__5 h1906an__6 h1911an__6 h1916an__6 h1921an__6 h1906an__7 h1911an__7 h1916an__7 h1921an__7
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel
	
saveold "$PUF\SPC_TON_2021_HIES_19a-AgricParcel_v01_PUF", version(12) replace

*	19b. HOUSEHOLD AGRIC VEGETABLES DATASET	*
use "$TEMP\SPC_TON_2021_HIES_19b-AgricultureVegetables_v02",clear

** Preliminary cleaning
	label data "s19b agriculture vegetables dataset - household"

preserve
	collapse (sum) h1907__12, by(anon_id07)
	save `temp1', replace
restore

** Recoding variables and recode the "0" as missing (.)
* 13 variables anonymized : h1907__2 h1907__3 h1907__4 h1907__5 h1907__10 h1907__12 h1907__13 h1908__2 h1908__3 h1908__5 h1908__10 h1908__12 h1908__13 
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h1907__2 (30/140=30) (9999=.)
		la def h1907__2			30 "30 or more"
		la val h1907__2 h1907__2
	recode h1907__3 (30/140=30)
		la def h1907__3			30 "30 or more"
		la val h1907__3 h1907__3
	recode h1907__4 (30/100=30)
		la def h1907__4			30 "30 or more"
		la val h1907__4 h1907__4
	recode h1907__5 (30/200=30)
		la def h1907__5			30 "30 or more"
		la val h1907__5 h1907__5
	recode h1907__10 (30/200=30)
		la def h1907__10		30 "30 or more"
		la val h1907__10 h1907__10
	recode h1907__12 (100/960=100)
		la def h1907__12		100 "100 or more"
		la val h1907__12 h1907__12
	recode h1907__13 (15/500=15)
		la def h1907__13		15 "15 or more"
		la val h1907__13 h1907__13
	recode h1908__2 (100 9999=.)
	recode h1908__3 (70/100=.)
	recode h1908__5 (60/120=.)
	recode h1908__10 (98/142=.)
	recode h1908__12 (20/120=20)
		la def h1908__12		20 "20 or more"
		la val h1908__12 h1908__12
	recode h1908__13 (100/150=.)

saveold "$PUF\SPC_TON_2021_HIES_19b-AgricVegetables_v01_PUF", version(12) replace

preserve
	collapse (sum) h1907__12, by(anon_id07)
	save `temp4', replace
restore

*	19c. HOUSEHOLD AGRIC ROOT CROP DATASET	*
use "$TEMP\SPC_TON_2021_HIES_19c-AgricultureRootcrops_v02",clear

** Preliminary cleaning
	label data "s19c agriculture root crop dataset - household"

preserve
	collapse (sum) h1912__5, by(anon_id07)
	save `temp2', replace
restore

** Recoding variables and recode the "0" as missing (.)
* 15 variables anonymized : h1912__1 h1912__2 h1912__3 h1912__4 h1912__5 h1912__6 h1912__8 h1912__9 h1913__1 h1913__2 h1913__3 h1913__4 h1913__5 h1913__6 h1913__8 
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h1912__1 (400/1500=400)
		la def h1912__1		400 "400 or more"
		la val h1912__1 h1912__1
	recode h1912__2 (200/2000=200)
		la def h1912__2		200 "200 or more"
		la val h1912__2 h1912__2
	recode h1912__3 (200/5160=200)
		la def h1912__3		200 "200 or more"
		la val h1912__3 h1912__3
	recode h1912__4 (500/2500=500)
		la def h1912__4		500 "500 or more"
		la val h1912__4 h1912__4
	recode h1912__5 (600/2510=600)
		la def h1912__5		600 "600 or more"
		la val h1912__5 h1912__5
	recode h1912__6 (200/1600=200)
		la def h1912__6		200 "200 or more"
		la val h1912__6 h1912__6
	recode h1912__8 (150/2000=150)
		la def h1912__8		150 "150 or more"
		la val h1912__8 h1912__8
	recode h1912__9 (180=.)
	recode h1913__1 (300/1500=300)
		la def h1913__1		300 "300 or more"
		la val h1913__1 h1913__1
	recode h1913__2 (150/2000=150)
		la def h1913__2		150 "150 or more"
		la val h1913__2 h1913__2
	recode h1913__3 (100/5000=100)
		la def h1913__3		100 "100 or more"
		la val h1913__3 h1913__3
	recode h1913__4 (100/2500=100)
		la def h1913__4		100 "100 or more"
		la val h1913__4 h1913__4
	recode h1913__5 (400/2500=400)
		la def h1913__5		400 "400 or more"
		la val h1913__5 h1913__5
	recode h1913__6 (150/1600=150)
		la def h1913__6		150 "150 or more"
		la val h1913__6 h1913__6
	recode h1913__8 (100/500=100)
		la def h1913__8		100 "100 or more"
		la val h1913__8 h1913__8

saveold "$PUF\SPC_TON_2021_HIES_19c-AgricRootCrop_v01_PUF", version(12) replace

preserve
	collapse (sum) h1912__5, by(anon_id07)
	save `temp5', replace
restore

*	19d. HOUSEHOLD AGRIC OTHER PLANT DATASET	*
use "$TEMP\SPC_TON_2021_HIES_19d-AgricultureOtherPlants_v02",clear

** Preliminary cleaning
	label data "s19d agriculture other plants dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 3 variables anonymized : h1917__3 h1918__3 h1917__5
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h1917__3 (200/700=200)
		la def h1917__3 200 "200 or more"
		la val h1917__3 h1917__3
	recode h1918__3 (150/700=150)
		la def h1918__3 150 "150 or more"
		la val h1918__3 h1918__3
	recode h1917__5 (100/400=100)
		la def h1917__5 100 "100 or more"
		la val h1917__5 h1917__5

saveold "$PUF\SPC_TON_2021_HIES_19d-AgricOtherPlants_v01_PUF", version(12) replace

*	19e. HOUSEHOLD AGRIC FRUIT DATASET	*
use "$TEMP\SPC_TON_2021_HIES_19e-AgricultureFruits_v02",clear

** Preliminary cleaning
	label data "s19e agriculture fruit dataset - household"

preserve
	collapse (sum) h1922__5, by(anon_id07)
	save `temp3', replace
restore

** Recoding variables and recode the "0" as missing (.)
* 6 variables anonymized : h1922__2 h1923__2 h1922__5 h1923__5 h1922__8 h1923__8
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	recode h1922__2 (100/500=100)
		la def h1922__2			100 "100 or more"
		la val h1922__2 h1922__2
	recode h1923__2 (100/500=100)
		la def h1923__2			100 "100 or more"
		la val h1923__2 h1923__2
	recode h1922__5 (600/1000=600)
		la def h1922__5			600 "600 or more"
		la val h1922__5 h1922__5
	recode h1923__5 (400/850=400)
		la def h1923__5			400 "400 or more"
		la val h1923__5 h1923__5
	recode h1922__8 (1000=.)
	recode h1923__8 (950=.)

saveold "$PUF\SPC_TON_2021_HIES_19e-AgricFruit_v01_PUF", version(12) replace

preserve
	collapse (sum) h1922__5, by(anon_id07)
	save `temp6', replace
restore

*	20. HOUSEHOLD LEGAL SERVICES DATASET	*
use "$TEMP\SPC_TON_2021_HIES_20-LegalServices_v02",clear

** Preliminary cleaning
	label data "s20 legal services dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 4 variables anonymized : h2001__5 h2001__6 h2001e h2001f
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	gen h2001__5_6=0
		replace h2001__5_6=1 if (h2001__5==1 | h2001__6==1)
		la var h2001__5_6 "ANON: legal services paid:fees for labour/commercial conflicts"
		order h2001__5_6, a(h2001__6)
		drop h2001__5 h2001__6
	egen h2001e_f=rowtotal(h2001e h2001f),m
		la var h2001e_f "ANON: amount paid for labour/commercial conflicts"
		order h2001e_f, a(h2001f)
		drop h2001e h2001f
	gen desc_2001e_f=""
		replace desc_2001e_f="Legal fees - labour or commercial" if (desc_2001e=="Legal fees - labour" | desc_2001f=="Legal fees - commercial")
		la var desc_2001e_f "ANON: item description"
		order desc_2001e_f, a(desc_2001f)
		drop desc_2001e desc_2001f
	gen coicop_2001e_f=.
		replace coicop_2001e_f=1270299005 if h2001e_f!=.
		la var coicop_2001e_f "ANON:coicop code"
		order coicop_2001e_f, a(coicop_2001f)
		drop coicop_2001e coicop_2001f
		format coicop_2001e_f %14.2g		

saveold "$PUF\SPC_TON_2021_HIES_20-LegalServices_v01_PUF", version(12) replace

*	AGG HOUSEHOLD DATASET 
**	Original	**
use "$TEMP\SPC_TON_2021_HIES_11-Dwelling_v02", clear
	keep anon_id07 fweight rururb island strata anon_district anon_village anon_block hhsize h1102 h1103 h1104 h1105 h1106 h1122h
	merge 1:m anon_id07 using "$TEMP\SPC_TON_2021_HIES_12-HouseholdAssets_v02", keepusing(h1202__1 h1202__2 h1202__3 h1202__4 h1202__6 h1202__8 h1202__13 h1202__24 h1202__25 h1202__26 h1202__30 h1202__32 h1202__34 h1202__36)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_TON_2021_HIES_13b-Vehicles_v02", keepusing(h13b2__1 h13b2__2 h13b2__4)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_TON_2021_HIES_13c-InternationalTravel_v02", keepusing(h13c3__1 h13c3__2)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_TON_2021_HIES_13d-DomesticTravel_v02", keepusing(h13d3__1 h13d3__2)
		drop _m	
	merge 1:1 anon_id07 using "$TEMP\SPC_TON_2021_HIES_18-LivestockAqua_v02", keepusing(h1802__1 h1802__2 h1802__3 h1802__4 h1802__5 h1802__6 h1802__7 h1805__3)
		drop _m
	merge 1:1 anon_id07 using `temp1', keepusing(h1907__12)
		drop _m
	merge 1:1 anon_id07 using `temp2', keepusing(h1912__5)
		drop _m
	merge 1:1 anon_id07 using `temp3', keepusing(h1922__5)
		drop _m
	merge 1:1 anon_id07 using "$TEMP\SPC_TON_2021_HIES_20-LegalServices_v02", keepusing(h2001__5 h2001__6 h2001e h2001f)
		drop _m

saveold "$TEMP\SPC_TON_2021_HIES_HouseholdAgg11-20_v02", version(12) replace
	
**	PUF	**	
use "$PUF\SPC_TON_2021_HIES_11-Dwelling_v01_PUF", clear
	keep anon_id07 fweight rururb island strata anon_district anon_village anon_block hhsize h1102 h1103 h1104 h1105 h1106 h1122h
	merge 1:m anon_id07 using "$PUF\SPC_TON_2021_HIES_12-HouseholdAssets_v01_PUF", keepusing(h1202__1 h1202__2 h1202__3 h1202__4 h1202__6 h1202__8 h1202__13 h1202__24 h1202__25 h1202__26 h1202__30 h1202__32 h1202__34 h1202__36)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_TON_2021_HIES_13b-Vehicles_v01_PUF", keepusing(h13b2__1 h13b2__2 h13b2__4)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_TON_2021_HIES_13c-InternationalTravel_v01_PUF", keepusing(h13c3__1 h13c3__2)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_TON_2021_HIES_13d-DomesticTravel_v01_PUF", keepusing(h13d3__1 h13d3__2)
		drop _m	
	merge 1:1 anon_id07 using "$PUF\SPC_TON_2021_HIES_18-LivestockAqua_v01_PUF", keepusing(h1802__1 h1802__2 h1802__3 h1802__4 h1802__5 h1802__6 h1802__7 h1805__3)
		drop _m
	merge 1:1 anon_id07 using `temp4', keepusing(h1907__12)
		drop _m
	merge 1:1 anon_id07 using `temp5', keepusing(h1912__5)
		drop _m
	merge 1:1 anon_id07 using `temp6', keepusing(h1922__5)
		drop _m
	merge 1:1 anon_id07 using "$PUF\SPC_TON_2021_HIES_20-LegalServices_v01_PUF", keepusing(h2001__5_6 h2001e_f)
		drop _m

saveold "$PUF\SPC_TON_2021_HIES_HouseholdAgg11-20_v01_PUF", version(12) replace

*	30. HOUSEHOLD EXPENDITURE DATASET	*
use "$TEMP\SPC_TON_2021_HIES_30-ExpenditureAggregate_v02",clear

** Preliminary cleaning
	label data "s30 expenditure aggregate dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variable anonymized : 
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

saveold "$PUF\SPC_TON_2021_HIES_30-ExpenditureAggreg_v01_PUF", version(12) replace

*	40. HOUSEHOLD INCOME DATASET	*
use "$TEMP\SPC_TON_2021_HIES_40-IncomeAggregate_v02",clear

** Preliminary cleaning
	label data "s40 income aggregate dataset - household"

** Recoding variables and recode the "0" as missing (.)
* 0 variables anonymized : 
	recode hhsize (15/25=15)
		label define hhsizel 	15 "15 people or more"
		label values hhsize hhsizel

	drop ind_desc_main occ_desc_main ind_desc_sec occ_desc_sec isco isic

saveold "$PUF\SPC_TON_2021_HIES_40-IncomeAggreg_v01_PUF", version(12) replace

*	-------------------------------------------------------------------------------------------------------------------------------
**	6. Run the post-anonymization datasets through the Statistical Disclosure Control Shiny GUI
*	-------------------------------------------------------------------------------------------------------------------------------
*	0. COVER DATASET	*
*		Risk of disclosure 1: 0.95% (20.3)
*		Risk of disclosure 2: % ()

*	AGG PERSON DATASET
*		Risk of disclosure 1: 19.77% (2,186.85)
*		Risk of disclosure 2: % ()

*	AGG HOUSEHOLD DATASET
*		Risk of disclosure 1: % ()
*		Risk of disclosure 2: 28.72% (611.74)
	
*	30. HOUSEHOLD EXPENDITURE DATASET	*
*		Risk of disclosure 1: 2.52% (4,523.73)
*		Risk of disclosure 2: % ()

*	40. HOUSEHOLD INCOME DATASET	*
*		Risk of disclosure 1: 3.11% (1,695.9)
*		Risk of disclosure 2: % ()
