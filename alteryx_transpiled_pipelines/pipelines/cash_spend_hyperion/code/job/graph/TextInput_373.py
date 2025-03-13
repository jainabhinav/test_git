from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_373(spark: SparkSession) -> DataFrame:
    schemaFields = StructType([
        StructField("Gl Account (# only)", StringType(), True), StructField("Gl Account", StringType(), True), StructField("Planning Account", StringType(), True), StructField("Category", StringType(), True), StructField("Regrouped Level 4", StringType(), True)
    ])\
        .fields
    readSchema = StructType([StructField(f.name, StringType(), True) for f in schemaFields])
    castExpressions = [col(f.name).cast(f.dataType) for f in schemaFields]
    df1 = spark.createDataFrame(
        [Row(
           "600010", 
           "600010 - Salaries & Wages", 
           "PA600010 - Salaries & Wages", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row("600011", "600011 - Overtime", "PA600011 - Overtime", "Labor and Benefits", "Labor & Benefits"),          Row(
           "600012", 
           "600012 - Cross Site Labor Charges", 
           "PA600012 - Salaries & Wages - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "600013", 
           "600013 - Amgen Staff - 13th Month Payroll Advance(S)", 
           "PA600012 - Salaries & Wages - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "600014", 
           "600014 - Other Salaries", 
           "PA600012 - Salaries & Wages - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "600015", 
           "600015 - Salaries Expatriates", 
           "PA600012 - Salaries & Wages - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601009", 
           "601009 - GMIP/VEP/IPIP Bonus Annual True Up", 
           "PA601009 - Bonuses & Incentives - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601010", 
           "601010 - GMIP/VEP/IPIP Bonus", 
           "PA601010 - GMIP / VEP / IPIP Bonus", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601011", 
           "601011 - Miscellaneous Bonuses & Other Compensation", 
           "PA601009 - Bonuses & Incentives - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601012", 
           "601012 - Stock Option Compensation", 
           "PA601012 - Stock Option Compensation", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601013", 
           "601013 - Restricted Stock Compensation", 
           "PA601013 - Restricted Stock Compensation", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601014", 
           "601014 - Long Term Compensation Plan", 
           "PA601014 - Long Term Compensation Plan", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601015", 
           "601015 - Retention Payments", 
           "PA601009 - Bonuses & Incentives - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601016", 
           "601016 - Severance/Staff Leave Indemnity", 
           "PA601009 - Bonuses & Incentives - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601017", 
           "601017 - IPIP Bonus", 
           "PA601010 - GMIP / VEP / IPIP Bonus", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row("601018", "601018 - Vacation Expense", "PA601018 - Vacation", "Labor and Benefits", "Labor & Benefits"),          Row(
           "601019", 
           "601019 - Untaken Vacations Days (S)", 
           "PA601018 - Vacation", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601020", 
           "601020 - Sick - Leave Payment (S)", 
           "PA601009 - Bonuses & Incentives - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601022", 
           "601022 - Global Recognition Program", 
           "PA601022 - Global Recognition Program", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "601023", 
           "601023 - Postretirement Benefits", 
           "PA601009 - Bonuses & Incentives - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "602010", 
           "602010 - Commissions / Sales Incentives", 
           "PA602010 - Commissions / Sales Incentives", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603010", 
           "603010 - Payroll Taxes Expense", 
           "PA603010 - Payroll Taxes", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603020", 
           "603020 - Payroll Taxes - Statutory I", 
           "PA603010 - Payroll Taxes", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603030", 
           "603030 - Payroll Taxes - Statutory II", 
           "PA603010 - Payroll Taxes", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603031", 
           "603031 - Statutory profit sharing (S)", 
           "PA603010 - Payroll Taxes", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603510", 
           "603510 - Defined contribution plan (employer contribution)", 
           "PA603510 - Benefits", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603511", 
           "603511 - Defined benefit plan (employer contribution)", 
           "PA603510 - Benefits", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603512", 
           "603512 - Workers Compensation Insurance Expense", 
           "PA603510 - Benefits", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603513", 
           "603513 - Group Insurance Expense", 
           "PA603510 - Benefits", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603515", 
           "603515 - Miscellaneous Other Staff Benefits", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603516", 
           "603516 - Misc Other Staff Benefits non-taxable", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "603527", 
           "603527 - Pension Plan TA CGIS (S)", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "604013", 
           "604013 - Accrued Payroll Taxes Expense", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "604014", 
           "604014 - Accrued 401K Employer Match Expense", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "606010", 
           "606010 - Mark to Market - Deferred Compensation Plan", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "606011", 
           "606011 - Mark to Market - Supplemental Retirement Plan (Ass", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "606012", 
           "606012 - Mark to Market - Supplemental Retirement Plan (Lia", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "606013", 
           "606013 - Mark to Market - Cash Value", 
           "PA603515 - Benefits - Other", 
           "Labor and Benefits", 
           "Labor & Benefits"
         ),          Row(
           "610010", 
           "610010 - Moving and Relocation Expenses", 
           "PA610010 - Recruiting and Relocation", 
           "Staffing (Mobility & Recruiting)", 
           "Staff Support"
         ),          Row(
           "610011", 
           "610011 - Recruiting Expenses", 
           "PA610010 - Recruiting and Relocation", 
           "Staffing (Mobility & Recruiting)", 
           "Staff Support"
         ),          Row(
           "610210", 
           "610210 - Office Supplies", 
           "PA610210 - Office Supplies & Postage", 
           "Facility Services", 
           "Staff Support"
         ),          Row(
           "610211", 
           "610211 - Operating Supplies", 
           "PA620215 - Legacy Core Non Core", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "610214", 
           "610214 - Low Value IS Equipment (S)", 
           "PA610210 - Office Supplies & Postage", 
           "Facility Services", 
           "Staff Support"
         ),          Row(
           "610215", 
           "610215 - IS Software, Hardware & Computer Supplies", 
           "PA610215 - IS Software/Hardware", 
           "IS Software/Hardware", 
           "FE&O"
         ),          Row(
           "610216", 
           "610216 - Lab Consumables & Services", 
           "PA610216 - Lab Consumables & Services", 
           "Lab Consumables & Services", 
           "Outside Expenses"
         ),          Row(
           "610220", 
           "610220 - OP Lease Expense - IS Equip", 
           "PA610215 - IS Software/Hardware", 
           "IS Software/Hardware", 
           "FE&O"
         ),          Row(
           "610221", 
           "610221 - Short Term Lease Expense - OP Lease - IS Equip", 
           "PA610215 - IS Software/Hardware", 
           "IS Software/Hardware", 
           "FE&O"
         ),          Row(
           "610223", 
           "610223 - Other Expense - OP Lease - IS Equip", 
           "PA610215 - IS Software/Hardware", 
           "IS Software/Hardware", 
           "FE&O"
         ),          Row(
           "610310", 
           "610310 - Miscellaneous Staff Support", 
           "PA610310 - Other Spend - Staff Support", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Staff Support"
         ),          Row(
           "610311", 
           "610311 - Food / Vending Services", 
           "PA610311 - Facility Services", 
           "Facility Services", 
           "Outside Expenses"
         ),          Row(
           "610314", 
           "610314 - Staff Support Statutory (S)", 
           "PA610310 - Other Spend - Staff Support", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Staff Support"
         ),          Row(
           "610315", 
           "610315 - Books, Subscriptions, Memberships, Dues", 
           "PA610315 - Research & Data Purchases and Services", 
           "Research & Data", 
           "Outside Expenses"
         ),          Row("610316", "610316 - Postage", "PA610210 - Office Supplies & Postage", "Facility Services", "Staff Support"),          Row(
           "610318", 
           "610318 - Food Services (S)", 
           "PA610310 - Other Spend - Staff Support", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Staff Support"
         ),          Row(
           "610320", 
           "610320 - OP Lease Expense - Other Equip", 
           "PA626010 - Facility/Equipment Repair & Maintenance", 
           "Facility Services", 
           "FE&O"
         ),          Row(
           "610321", 
           "610321 - Short Term Lease Expense - OP Lease - Other Equip", 
           "PA626010 - Facility/Equipment Repair & Maintenance", 
           "Facility Services", 
           "FE&O"
         ),          Row(
           "610322", 
           "610322 - Variable Lease Expense - OP Lease - Other Equip", 
           "PA626010 - Facility/Equipment Repair & Maintenance", 
           "Facility Services", 
           "FE&O"
         ),          Row(
           "610324", 
           "610324 - Variable Lease Expense - FN Lease - Other Equip", 
           "PA621751 - Equipment Purchases (Capital/Non-Capital)", 
           "Equipment", 
           "Outside Expenses"
         ),          Row("610510", "610510 - Telecommunications and Data Charges", "PA610510 - Telecom", "Telecom", "Staff Support"),          Row("610512", "610512 - Portable/Cell Phone Expenses", "PA610510 - Telecom", "Telecom", "Staff Support"),          Row(
           "610610", 
           "610610 - Company Sponsored Activities - Not Work Related", 
           "PA610310 - Other Spend - Staff Support", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Staff Support"
         ),          Row(
           "610611", 
           "610611 - Training and Development Expense", 
           "PA610611 - Training and Development", 
           "Training", 
           "Staff Support"
         ),          Row(
           "610612", 
           "610612 - Ex-Patriate Expenses", 
           "PA610612 - Ex-Patriate", 
           "Staffing (Mobility & Recruiting)", 
           "Staff Support"
         ),          Row(
           "610699", 
           "610699 - Staff Support Allocation", 
           "PA610310 - Other Spend - Staff Support", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Staff Support"
         ),          Row(
           "615010", 
           "615010 - Business/Travel Meals & Entertainment", 
           "PA615010 - Travel for Amgen Staff", 
           "Travel", 
           "Staff Support"
         ),          Row(
           "615011", 
           "615011 - Healthcare Professionals Meals & Expenses", 
           "PA615010 - Travel for Amgen Staff", 
           "Travel", 
           "Staff Support"
         ),          Row(
           "615012", 
           "615012 - Business/Travel Meals & Entert I (S)", 
           "PA615010 - Travel for Amgen Staff", 
           "Travel", 
           "Staff Support"
         ),          Row(
           "615013", 
           "615013 - Business/Travel Meals & Entert II (S)", 
           "PA615010 - Travel for Amgen Staff", 
           "Travel", 
           "Staff Support"
         ),          Row("615050", "615050 - Airfare", "PA615010 - Travel for Amgen Staff", "Travel", "Staff Support"),          Row("615100", "615100 - Hotel/Lodging", "PA615010 - Travel for Amgen Staff", "Travel", "Staff Support"),          Row(
           "615150", 
           "615150 - Travel - Transportation Related Expense", 
           "PA615010 - Travel for Amgen Staff", 
           "Travel", 
           "Staff Support"
         ),          Row("615152", "615152 - Other Travel Expenses", "PA615010 - Travel for Amgen Staff", "Travel", "Staff Support"),          Row(
           "615153", 
           "615153 - VAT for Private Use Company Car (S)", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615154", 
           "615154 - Transportation related - Statutory II", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615200", 
           "615200 - Department Staff Activities (Activity + Meals)", 
           "PA615010 - Travel for Amgen Staff", 
           "Travel", 
           "Staff Support"
         ),          Row(
           "615250", 
           "615250 - Corporate Aircraft Expenses", 
           "PA615010 - Travel for Amgen Staff", 
           "Travel", 
           "Staff Support"
         ),          Row(
           "615300", 
           "615300 - Auto Fleet Rental", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615301", 
           "615301 - Car Related Costs Other I (S)", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615302", 
           "615302 - Car Related Costs Other II (S)", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615310", 
           "615310 - OP Lease Expense - Fleet", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615311", 
           "615311 - Short Term Lease Expense - OP Lease - Fleet", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615312", 
           "615312 - Variable Lease Expense - OP Lease - Fleet", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row(
           "615313", 
           "615313 - Other Expense - OP Lease - Fleet", 
           "PA615153 - Auto Fleet Rental & Other Car Expenses", 
           "Fleet", 
           "Staff Support"
         ),          Row("620010", "620010 - Consultant Expense", "PA620010 - Consulting", "Consulting", "Outside Expenses"),          Row(
           "620011", 
           "620011 - Consultant Expense (Project Related)", 
           "PA620010 - Consulting", 
           "Consulting", 
           "Outside Expenses"
         ),          Row(
           "620110", 
           "620110 - Temporaries Expense", 
           "PA620110 - Staff Augmentation", 
           "Staff Augmentation", 
           "Outside Expenses"
         ),          Row(
           "620111", 
           "620111 - Temporaries Expense (Project Related)", 
           "PA620110 - Staff Augmentation", 
           "Staff Augmentation", 
           "Outside Expenses"
         ),          Row(
           "620151", 
           "620151 - CT - Contractors (Contingent Workforce)", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "620210", 
           "620210 - External Services - Core Work", 
           "PA620215 - Legacy Core Non Core", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "620213", 
           "620213 - FSP Clinical Trial Services", 
           "PA620213 - FSP Clinical Trial Services", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "620214", 
           "620214 - Contract Sales Services", 
           "PA620214 - Contract Sales Services", 
           "Sales Support", 
           "Outside Expenses"
         ),          Row(
           "620215", 
           "620215 - External Services - Non-Core", 
           "PA620215 - Legacy Core Non Core", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "620216", 
           "620216 - Animals/Biology/Chemistry/Biospecimen Scientific S", 
           "PA620216 - Scientific Services", 
           "Scientific Services", 
           "Outside Expenses"
         ),          Row(
           "620217", 
           "620217 - Clinical Trial Services (excludes FSP)", 
           "PA620213 - FSP Clinical Trial Services", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "620218", 
           "620218 - Facility Services", 
           "PA610311 - Facility Services", 
           "Facility Services", 
           "Outside Expenses"
         ),          Row(
           "620219", 
           "620219 - IS Services", 
           "PA620219 - Technology Services", 
           "Technology Services", 
           "Outside Expenses"
         ),          Row(
           "620220", 
           "620220 - Financial Services", 
           "PA620220 - Financial Services", 
           "Financial Services", 
           "Outside Expenses"
         ),          Row(
           "620251", 
           "620251 - Contract Manufacturing Services Expense", 
           "PA620251 - Contract Manufacturing", 
           "Contract Manufacturing", 
           "Outside Expenses"
         ),          Row(
           "620252", 
           "620252 - Contract Mfg Direct Expenses", 
           "PA627610 - Cost of Sales Allocation", 
           "Non-External Expense", 
           "Allocations"
         ),          Row("620310", "620310 - Conferences and Meetings", "PA620310 - Meetings", "Meetings", "Outside Expenses"),          Row("620311", "620311 - Conferences/Meetings Meals", "PA620310 - Meetings", "Meetings", "Outside Expenses"),          Row("620312", "620312 - Non Amgen Personnel Travel", "PA620310 - Meetings", "Meetings", "Outside Expenses"),          Row(
           "620410", 
           "620410 - Sales & Marketing Promotional", 
           "PA620215 - Legacy Core Non Core", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "620411", 
           "620411 - Promotional & Advertising NonTax Deductible (Stat)", 
           "PA620210 - Other Spend - OSE", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "620412", 
           "620412 - Promotional Expenses Miscellaneous", 
           "PA620215 - Legacy Core Non Core", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "620415", 
           "620415 - Sponsorship", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row("620419", "620419 - Media - TV", "PA620419 - Media - TV", "Media", "Outside Expenses"),          Row("620420", "620420 - Media - Non-TV Consumer", "PA620420 - Media - Digital", "Media", "Outside Expenses"),          Row("620421", "620421 - Media - Search", "PA620421 - Media - Search", "Media", "Outside Expenses"),          Row("620422", "620422 - Media - Social", "PA620422 - Media - Social", "Media", "Outside Expenses"),          Row("620423", "620423 - Media - Non-TV HCP", "PA620423 - Media - Out of home", "Media", "Outside Expenses"),          Row("620424", "620424 - Media - Print, Audio, Other", "PA620410 - Media", "Media", "Outside Expenses"),          Row(
           "620425", 
           "620425 - Marketing Samples", 
           "PA620425 - Marketing Samples", 
           "Non-External Expense", 
           "Outside Expenses"
         ),          Row(
           "620426", 
           "620426 - Replacement Cost", 
           "PA620425 - Marketing Samples", 
           "Non-External Expense", 
           "Outside Expenses"
         ),          Row("620451", "620451 - Trade Shows / Exhibits", "PA620310 - Meetings", "Meetings", "Outside Expenses"),          Row(
           "620455", 
           "620455 - Outside Expense Gifts Deductible (S)", 
           "PA620310 - Meetings", 
           "Meetings", 
           "Outside Expenses"
         ),          Row(
           "620456", 
           "620456 - Outside Expenses - HCP Meals/Expenses", 
           "PA620310 - Meetings", 
           "Meetings", 
           "Outside Expenses"
         ),          Row(
           "620457", 
           "620457 - Outside Exp Recharge Entertain/Gift Settlement (S)", 
           "PA620310 - Meetings", 
           "Meetings", 
           "Outside Expenses"
         ),          Row(
           "620510", 
           "620510 - Advertising Expenses - Print", 
           "PA620215 - Legacy Core Non Core", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row("620518", "620518 - Media", "PA620410 - Media", "Media", "Outside Expenses"),          Row(
           "620519", 
           "620519 - Agencies - Creative Content", 
           "PA620512 - Agencies - Creative Content", 
           "Agencies", 
           "Outside Expenses"
         ),          Row(
           "620520", 
           "620520 - Print & Fulfillment", 
           "PA620510 - Print & Fulfillment", 
           "Print & Fulfillment", 
           "Outside Expenses"
         ),          Row(
           "620551", 
           "620551 - Scientific&Value Communications & Public Relations", 
           "PA620512 - Agencies - Creative Content", 
           "Agencies", 
           "Outside Expenses"
         ),          Row(
           "620558", 
           "620558 - Value Based Partnerships (Ex-US)", 
           "PA620512 - Agencies - Creative Content", 
           "Agencies", 
           "Outside Expenses"
         ),          Row(
           "620610", 
           "620610 - Public Communications - Product Related", 
           "PA620512 - Agencies - Creative Content", 
           "Agencies", 
           "Outside Expenses"
         ),          Row("620611", "620611 - Call Center Expenses", "PA620611 - Call Centers", "Call Centers", "Outside Expenses"),          Row(
           "620651", 
           "620651 - Grants", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row("620710", "620710 - Symposia/Workshop/C&E", "PA620310 - Meetings", "Meetings", "Outside Expenses"),          Row("620716", "620716 - Symposia - Honoraria (S)", "PA620310 - Meetings", "Meetings", "Outside Expenses"),          Row(
           "620751", 
           "620751 - Grants/Donations to HCP or Scientist Institutions", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "620760", 
           "620760 - Independent Medical Education", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "620810", 
           "620810 - Compassionate Goods", 
           "PA620810 - Compassionate Goods", 
           "Non-External Expense", 
           "Outside Expenses"
         ),          Row(
           "620811", 
           "620811 - Compassionate Goods (Automatic)", 
           "PA620810 - Compassionate Goods", 
           "Non-External Expense", 
           "Outside Expenses"
         ),          Row(
           "620851", 
           "620851 - Data & Information - Goods & Services", 
           "PA610315 - Research & Data Purchases and Services", 
           "Research & Data", 
           "Outside Expenses"
         ),          Row(
           "620911", 
           "620911 - Profit Share", 
           "PA620910 - Gross Profit Share - Other", 
           "Non-External Expense", 
           "Outside Expenses"
         ),          Row(
           "620912", 
           "620912 - Partnership Residual Royalties", 
           "PA620912 - Partnership Residual Royalties", 
           "Non-External Expense", 
           "Outside Expenses"
         ),          Row(
           "620951", 
           "620951 - US Branded Prescription Drug Fee", 
           "PA620951 - US Branded Prescription Drug Fee", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "621010", 
           "621010 - MilestonePmnts/SigningFeesExp/RDInvestExp", 
           "PA621010 - Milestone Payments", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "621110", 
           "621110 - Partnership FTE Expenses", 
           "PA621110 - Partnership Other", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "621111", 
           "621111 - Partnership Outside Expenses", 
           "PA621110 - Partnership Other", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "621310", 
           "621310 - Preclinical Trial Expenses", 
           "PA621310 - Preclinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "621410", 
           "621410 - CT - Patient Expenses - 30/70", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "621411", 
           "621411 - CT - Patient Expenses - Straight Line", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row("621451", "621451 - CT -  Monitoring", "PA620151 - Clinical Trials", "Clinical Trials", "Outside Expenses"),          Row(
           "621454", 
           "621454 - CT - Fully OutSource", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "621455", 
           "621455 - CT - Health Economics", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "621456", 
           "621456 - CT-Observational Research", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row("621458", "621458 - CT - Safety (S)", "PA620151 - Clinical Trials", "Clinical Trials", "Outside Expenses"),          Row("621510", "621510 - CT - Central Lab", "PA620151 - Clinical Trials", "Clinical Trials", "Outside Expenses"),          Row("621511", "621511 - CT - IVRS", "PA620151 - Clinical Trials", "Clinical Trials", "Outside Expenses"),          Row(
           "621512", 
           "621512 - CT - Speciality Labs", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row("621514", "621514 - CT - Imaging", "PA620151 - Clinical Trials", "Clinical Trials", "Outside Expenses"),          Row("621515", "621515 - CT - Biomarkers", "PA620151 - Clinical Trials", "Clinical Trials", "Outside Expenses"),          Row("621516", "621516 - CT - IST", "PA620151 - Clinical Trials", "Clinical Trials", "Outside Expenses"),          Row("621551", "621551 - Clinical Trial Meetings", "PA620310 - Meetings", "Meetings", "Outside Expenses"),          Row(
           "621552", 
           "621552 - Feasibility Study Costs", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "621553", 
           "621553 - Clinical Other Costs", 
           "PA620151 - Clinical Trials", 
           "Clinical Trials", 
           "Outside Expenses"
         ),          Row(
           "621554", 
           "621554 - Clinical MFG Services - Contract Manufacturing", 
           "PA621554 - Clinical Contract Manufacturing", 
           "Contract Manufacturing", 
           "Outside Expenses"
         ),          Row(
           "621555", 
           "621555 - Clinical Mfg Materials - Comparator Drugs", 
           "PA621555 - Clinical - Comparator Drugs", 
           "Non-Amgen Medicinal Products", 
           "Outside Expenses"
         ),          Row(
           "621610", 
           "621610 - Comparative Animal Research", 
           "PA620216 - Scientific Services", 
           "Scientific Services", 
           "Outside Expenses"
         ),          Row(
           "621710", 
           "621710 - Manufacturing Supplies", 
           "PA620215 - Legacy Core Non Core", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "621711", 
           "621711 - Shipping Supplies", 
           "PA621711 - Distribution and Logistics", 
           "Distribution", 
           "Outside Expenses"
         ),          Row(
           "621712", 
           "621712 - Raw Materials & Manufacturing Consumables", 
           "PA610211 - Raw Materials & Mfg Consumables", 
           "Raw Materials & Manufacturing Consumables", 
           "Outside Expenses"
         ),          Row(
           "621713", 
           "621713 - Drug Delivery Devices - Goods & Services", 
           "PA621713 - Drug Delivery Devices - Goods & Services", 
           "Devices", 
           "Outside Expenses"
         ),          Row(
           "621714", 
           "621714 - Product Packaging - Goods & Services", 
           "PA610211 - Raw Materials & Mfg Consumables", 
           "Raw Materials & Manufacturing Consumables", 
           "Outside Expenses"
         ),          Row(
           "621751", 
           "621751 - Equipment Purchases (Capital/Non-Capital)", 
           "PA621751 - Equipment Purchases (Capital/Non-Capital)", 
           "Equipment", 
           "Outside Expenses"
         ),          Row(
           "621752", 
           "621752 - Spare Parts for Equipment", 
           "PA621752 - Equipment Spare Parts", 
           "Facility Services", 
           "Outside Expenses"
         ),          Row(
           "621754", 
           "621754 - Short Term Lease Expense -OP Lease -Factory Equip", 
           "PA621751 - Equipment Purchases (Capital/Non-Capital)", 
           "Equipment", 
           "Outside Expenses"
         ),          Row(
           "621755", 
           "621755 - Variable Lease Expense - OP Lease - Factory Equip", 
           "PA621751 - Equipment Purchases (Capital/Non-Capital)", 
           "Equipment", 
           "Outside Expenses"
         ),          Row(
           "621810", 
           "621810 - Material Handling", 
           "PA610211 - Raw Materials & Mfg Consumables", 
           "Raw Materials & Manufacturing Consumables", 
           "Outside Expenses"
         ),          Row(
           "621851", 
           "621851 - Freight / Shipping / Distribution / Warehousing", 
           "PA621711 - Distribution and Logistics", 
           "Distribution", 
           "Outside Expenses"
         ),          Row(
           "621852", 
           "621852 - Freight In", 
           "PA621711 - Distribution and Logistics", 
           "Distribution", 
           "Outside Expenses"
         ),          Row(
           "621855", 
           "621855 - Shipping Samples", 
           "PA621711 - Distribution and Logistics", 
           "Distribution", 
           "Outside Expenses"
         ),          Row(
           "621910", 
           "621910 - Inventory Adjustments", 
           "PA620252 - Capitalized Overhead", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "621911", 
           "621911 - Product/Raw Material Sampling", 
           "PA610211 - Raw Materials & Mfg Consumables", 
           "Raw Materials & Manufacturing Consumables", 
           "Outside Expenses"
         ),          Row(
           "622010", 
           "622010 - Legal Fees - Litigation", 
           "PA622010 - Legal & Patent Fees", 
           "Legal", 
           "Outside Expenses"
         ),          Row("622110", "622110 - Patent Fees", "PA622010 - Legal & Patent Fees", "Legal", "Outside Expenses"),          Row(
           "622210", 
           "622210 - Legal Fees - Non Litigation", 
           "PA622010 - Legal & Patent Fees", 
           "Legal", 
           "Outside Expenses"
         ),          Row(
           "622211", 
           "622211 - Statutory Auditors Fee", 
           "PA620220 - Financial Services", 
           "Financial Services", 
           "Outside Expenses"
         ),          Row(
           "622451", 
           "622451 - Shareholder / Board of Directors Support", 
           "PA620210 - Other Spend - OSE", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622510", 
           "622510 - Donations", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622520", 
           "622520 - Donations \u2013 Members of Healthcare Community", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622521", 
           "622521 - Donations \u2013 Patient Advocacy Orgs", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622522", 
           "622522 - Donations \u2013 NON-Members of Healthcare Community", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622551", 
           "622551 - Professional / Regulatory Fees", 
           "PA622551 - Professional / Regulatory Fees", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622560", 
           "622560 - Corp Sponsorships and Corp Memberships", 
           "PA620415 - Grants & Donations", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622610", 
           "622610 - Insurance Premiums", 
           "PA626410 - Facilities Insurance & Insurance Premiums", 
           "Financial Services", 
           "FE&O"
         ),          Row(
           "622651", 
           "622651 - Franchise Taxes", 
           "PA620210 - Other Spend - OSE", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622710", 
           "622710 - VAT Taxes", 
           "PA620210 - Other Spend - OSE", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622751", 
           "622751 - Other Business Taxes and Fees", 
           "PA620210 - Other Spend - OSE", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622810", 
           "622810 - Bad Debt Expense", 
           "PA620210 - Other Spend - OSE", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622821", 
           "622821 - Prepaid Amortization OSE Other", 
           "PA620210 - Other Spend - OSE", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "Outside Expenses"
         ),          Row(
           "622822", 
           "622822 - Prepaid Amortization OSE IS Hardware & Software", 
           "PA610215 - IS Software/Hardware", 
           "IS Software/Hardware", 
           "FE&O"
         ),          Row(
           "622851", 
           "622851 - Intangible Asset Amort Exp", 
           "PA622851 - Intangible Asset Amort Exp", 
           "Non-External Expense", 
           "Outside Expenses"
         ),          Row(
           "623010", 
           "623010 - Machinery and Equipment Depreciation Expense", 
           "PA623010 - Machinery & Equipment Depreciation", 
           "Non-External Expense", 
           "FE&O"
         ),          Row(
           "623011", 
           "623011 - Depreciation Expense - Software (S)", 
           "PA623010 - Machinery & Equipment Depreciation", 
           "Non-External Expense", 
           "FE&O"
         ),          Row(
           "623014", 
           "623014 - Prepaid Amortization FEO IS Hardware & Software Ma", 
           "PA610215 - IS Software/Hardware", 
           "IS Software/Hardware", 
           "FE&O"
         ),          Row(
           "623015", 
           "623015 - Prepaid Amortization FEO Other", 
           "PA610610 - Other Spend - FE&O", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "FE&O"
         ),          Row(
           "623017", 
           "623017 - Depreciation Expense - FN Lease - Factory Equip", 
           "PA623010 - Machinery & Equipment Depreciation", 
           "Non-External Expense", 
           "FE&O"
         ),          Row(
           "623018", 
           "623018 - Depreciation Expense - FN Lease - Other Equip", 
           "PA623010 - Machinery & Equipment Depreciation", 
           "Non-External Expense", 
           "FE&O"
         ),          Row(
           "624010", 
           "624010 - Building Related Depreciation Expense", 
           "PA624010 - Building Depreciation", 
           "Non-External Expense", 
           "FE&O"
         ),          Row(
           "625010", 
           "625010 - Asset Write-off Gain Loss", 
           "PA625010 - Asset Disposal Write-off", 
           "Non-External Expense", 
           "FE&O"
         ),          Row(
           "626010", 
           "626010 - Equipment Repairs & Maintenance", 
           "PA626010 - Facility/Equipment Repair & Maintenance", 
           "Facility Services", 
           "FE&O"
         ),          Row(
           "626110", 
           "626110 - Facility Repairs & Maintenance", 
           "PA626010 - Facility/Equipment Repair & Maintenance", 
           "Facility Services", 
           "FE&O"
         ),          Row(
           "626210", 
           "626210 - Computer Repairs & Maintenance", 
           "PA610215 - IS Software/Hardware", 
           "IS Software/Hardware", 
           "FE&O"
         ),          Row("626310", "626310 - Utility Expenses", "PA626310 - Utilities", "Utilities", "FE&O"),          Row(
           "626351", 
           "626351 - Property Tax Expense", 
           "PA626351 - Property TAX", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "FE&O"
         ),          Row(
           "626410", 
           "626410 - Facilities Insurance Expense", 
           "PA626410 - Facilities Insurance & Insurance Premiums", 
           "Financial Services", 
           "FE&O"
         ),          Row("626451", "626451 - Rent Expense", "PA626451 - Real Estate Rent / Leases", "Real Estate", "FE&O"),          Row(
           "626461", 
           "626461 - OP Lease Expense - Real Estate", 
           "PA626451 - Real Estate Rent / Leases", 
           "Real Estate", 
           "FE&O"
         ),          Row(
           "626462", 
           "626462 - Short Term Lease Expense - OP Lease - Real Estate", 
           "PA626451 - Real Estate Rent / Leases", 
           "Real Estate", 
           "FE&O"
         ),          Row(
           "626463", 
           "626463 - Variable Lease Expense - OP Lease - Real Estate", 
           "PA626451 - Real Estate Rent / Leases", 
           "Real Estate", 
           "FE&O"
         ),          Row(
           "626464", 
           "626464 - Other Expense - OP Lease - Real Estate", 
           "PA626451 - Real Estate Rent / Leases", 
           "Real Estate", 
           "FE&O"
         ),          Row(
           "626465", 
           "626465 - Sub-Lease Income - Real Estate", 
           "PA626451 - Real Estate Rent / Leases", 
           "Real Estate", 
           "FE&O"
         ),          Row(
           "626510", 
           "626510 - Capital Project Expenditures", 
           "PA626510 - Capital Project Expenditures", 
           "Construction", 
           "FE&O"
         ),          Row(
           "626599", 
           "626599 - Occupancy Allocation", 
           "PA610610 - Other Spend - FE&O", 
           "Partnerships, Sponsorships and Other Non-Market Facing", 
           "FE&O"
         ),          Row(
           "627010", 
           "627010 - Clinical Manufacturing Inventory Expense (Automati", 
           "PA627110 - Clinical Manufacturing", 
           "Raw Materials & Manufacturing Consumables", 
           "Clinical Mfg"
         ),          Row(
           "627011", 
           "627011 - Clinical Manufacturing Excess Capacity (Manual)", 
           "PA627110 - Clinical Manufacturing", 
           "Raw Materials & Manufacturing Consumables", 
           "Clinical Mfg"
         ),          Row(
           "627012", 
           "627012 - Clinical MFG Inventory Expense - WBS", 
           "PA627110 - Clinical Manufacturing", 
           "Raw Materials & Manufacturing Consumables", 
           "Clinical Mfg"
         ),          Row(
           "627110", 
           "627110 - Clinical Mfg Inventory Adjustment", 
           "PA627110 - Clinical Manufacturing", 
           "Raw Materials & Manufacturing Consumables", 
           "Clinical Mfg"
         ),          Row(
           "627510", 
           "627510 - Capitalized Overhead", 
           "PA620252 - Capitalized Overhead", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "627520", 
           "627520 - Capitalized Interest", 
           "PA620252 - Capitalized Overhead", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "627610", 
           "627610 - Cost of Sales Allocations", 
           "PA627610 - Cost of Sales Allocation", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "628010", 
           "628010 - Cost Recovery  - General (non study/non KA)", 
           "PA628010 - Non-Study Recoveries", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "628011", 
           "628011 - Cost Recovery - FTE Expenses", 
           "PA628011 - FTE Recoveries", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "628012", 
           "628012 - Cost Recovery - Adjustments", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "628016", 
           "628016 - Cost Recovery \u2013 Study", 
           "PA628014 - Study Recoveries", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "628017", 
           "628017 - Cost Recovery \u2013 FSP", 
           "PA628015 - FSP Recoveries", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "630010", 
           "630010 - Administrative Recovery of Costs", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "631010", 
           "631010 - R & D Cost Sharing Activities", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "632010", 
           "632010 - Rebates / Discounts (Automatic Posting)", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "632011", 
           "632011 - Rebates / Discounts (Manual Posting)", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "632012", 
           "632012 - VAT Recovered", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "632013", 
           "632013 - Small Difference Write Off Account for A/P", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "632014", 
           "632014 - Rebates Transitory (S)", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "632015", 
           "632015 - Small Difference Write Off Account for A/R", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "632016", 
           "632016 - Small Difference Write Off Account for A/R(Manual)", 
           "PA628012 - Cost Recoveries Other", 
           "Non-External Expense", 
           "Recoveries"
         ),          Row(
           "699911", 
           "699911 - FI - CO Reconciliation Account - Manual", 
           "PA699910 - Miscellaneous Clearing Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "900002", 
           "900002 - WBS Depreciation Settlement", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "900004", 
           "900004 - WBS Outside Expense Settlement", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "921710", 
           "921710 - Mfg Supplies Settlement", 
           "PA610211 - Raw Materials & Mfg Consumables", 
           "Raw Materials & Manufacturing Consumables", 
           "Outside Expenses"
         ),          Row(
           "940001", 
           "940001 - Perform Machinery", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "940002", 
           "940002 - Perform Labor", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "950001", 
           "950001 - Labor & Benefits", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "950002", 
           "950002 - Staff Support", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "950003", 
           "950003 - Outside Expenses", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row("950004", "950004 - External Mfg", "PA900001 - Allocation Accounts", "Non-External Expense", "Allocations"),          Row(
           "950005", 
           "950005 - Supplies & Materials", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row("950006", "950006 - Depreciation", "PA950006 - Depreciation", "Non-External Expense", "Allocations"),          Row("950007", "950007 - Utilities", "PA950007 - Utilities", "Non-External Expense", "Allocations"),          Row(
           "950008", 
           "950008 - Rent, Taxes & Insur", 
           "PA950008 - Rent-Taxes-Insurance", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "950009", 
           "950009 - Repairs & Maint.", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row(
           "950010", 
           "950010 - Stock Options", 
           "PA900001 - Allocation Accounts", 
           "Non-External Expense", 
           "Allocations"
         ),          Row("960006", "960006 - Depreciation", "PA960006 - Depreciation", "Non-External Expense", "FE&O"),          Row("960007", "960007 - Utilities", "PA960007 - Utilities", "Non-External Expense", "FE&O"),          Row(
           "960008", 
           "960008 - Rent, Taxes & Insur", 
           "PA960008 - Rent-Taxes-Insurance", 
           "Non-External Expense", 
           "FE&O"
         ),          Row("960011", "960011 - Healthcare", "PA603515 - Benefits - Other", "Labor and Benefits", "Labor & Benefits"),          Row("960012", "960012 - RMSA", "PA603515 - Benefits - Other", "Labor and Benefits", "Labor & Benefits")],
        readSchema
    )

    return df1.select(castExpressions)
