import pandas as pd
import os

path = "./data/NPRA SPONSOR DETAILS DECEMBER 2022/"

column_names=["TRUSTEE_ID",	"NAME_OF_TRUSTEE",	"SCHEME_ID", "SCHEME_TYPE",	"SCHEME_NAME", "EMPLOYER_NAME",	"EMPLOYER_SSNIT_NUMBER", "EMAIL_ADDRESS", "POSTAL_ADDRESS", "BUSINESS_LOCATION", "TELEPHONE_NUMBER", "EMPLOYEE_STRENGTH", "date of enrolment"]

template_df = pd.DataFrame([], columns=column_names)

arr = os.listdir("data/NPRA SPONSOR DETAILS DECEMBER 2022")[1:]
df = pd.read_excel(path + arr[0])
for file_path in arr[1:]:
    df_change = pd.read_excel(path + file_path)
    df = pd.concat([df, df_change])

template_df["NAME_OF_TRUSTEE"] = df["Name Of Trustee"]
template_df["SCHEME_TYPE"] = df["Type Of Scheme"]
template_df["SCHEME_NAME"] = df["Employer Name"]
template_df["EMPLOYER_SSNIT_NUMBER"] = df["Employer Snnit No"]
template_df["POSTAL_ADDRESS"] = df["Postal Address"]
template_df["BUSINESS_LOCATION"] = df["Business Location"]
template_df["TELEPHONE_NUMBER"] = df["Telephone No"]
template_df["EMPLOYEE_STRENGTH"] = df["Employee Strength"]
template_df.to_excel("template for Employer Enrolment.xls", index=False)