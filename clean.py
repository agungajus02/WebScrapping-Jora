import pandas as pd

# 1. Baca file CSV
file_path = "jora_sharded.csv"   # ganti sesuai lokasi file kamu
df = pd.read_csv(file_path)

# 2. Daftar occupation IT + variasi keyword umum
occupation_keywords = {
    'Analyst Programmer (261311)': ['analyst programmer'],
    'Software Engineer (261313)': ['software engineer', 'software developer', 'application developer', 'app developer'],
    'Developer Programmer (261312)': ['developer programmer', 'programmer', 'developer', 'full stack', 'backend', 'frontend'],
    'Software Tester (261314)': ['software tester', 'qa tester', 'quality assurance', 'test engineer'],
    'ICT Business Analyst (261111)': ['ict business analyst', 'business analyst'],
    'ICT Account Manager (225211)': ['ict account manager', 'account manager it'],
    'ICT Business Development Manager (225212)': ['ict business development manager', 'it business development'],
    'ICT Customer Support Officer (313112)': ['ict customer support', 'customer support it'],
    'ICT Managers nec (135199)': ['ict manager', 'it manager'],
    'ICT Project Manager (135112)': ['ict project manager', 'it project manager', 'project manager it'],
    'ICT Quality Assurance Engineer (263211)': ['quality assurance engineer', 'qa engineer'],
    'ICT Sales Representative (225213)': ['ict sales', 'it sales', 'sales engineer it'],
    'ICT Security Specialist (262112)': ['ict security specialist', 'cyber security', 'security analyst', 'information security'],
    'ICT Support Engineer (263212)': ['ict support engineer', 'support engineer it'],
    'ICT Support Technicians nec (313199)': ['it support', 'service desk', 'helpdesk', 'desktop support', 'technical support'],
    'ICT Systems Test Engineer (263213)': ['systems test engineer'],
    'ICT Trainer (223211)': ['ict trainer', 'it trainer'],
    'Network Administrator (263112)': ['network administrator', 'network admin'],
    'Network Analyst (263113)': ['network analyst'],
    'Computer Network and Systems Engineer (263111)': ['network and systems engineer', 'network engineer', 'systems engineer'],
    'Systems Administrator (262113)': ['systems administrator', 'sysadmin', 'windows admin', 'linux admin'],
    'Systems Analyst (261112)': ['systems analyst', 'system analyst'],
    'Web Administrator (313113)': ['web administrator'],
    'Web Designer (232414)': ['web designer', 'ui designer', 'ux designer', 'graphic designer web'],
    'Web Developer (261212)': ['web developer', 'frontend developer', 'wordpress developer'],
    'Database Administrator (262111)': ['database administrator', 'dba', 'sql dba'],
    'Data Scientist (261111)': ['data scientist', 'machine learning engineer', 'ai engineer'],
    'Cyber Security Specialist (262112)': ['cyber security', 'information security', 'security consultant'],
    'ICT Support and Test Engineers nec (263299)': ['support engineer', 'test engineer it'],
    'Information and Organisation Professionals nec (224999)': ['information professional', 'organisation professional'],
    'Cabler (Data and Telecommunications) (342411)': ['cabler', 'telecommunications cabler', 'data cabler']
}

# 3. Fungsi pencocokan title dengan keyword
def find_occupation(title):
    if pd.isna(title):
        return None
    title_lower = title.lower()
    for occ, keywords in occupation_keywords.items():
        for kw in keywords:
            if kw in title_lower:
                return occ
    return None

# 4. Tambahkan kolom occupation
df['occupation'] = df['title'].apply(find_occupation)

# 5. Filter hanya baris yang match occupation IT
df_it = df[df['occupation'].notna()]

# 6. Simpan ke Excel, 1 sheet per occupation
output_path = "filtered_it_jobs_full.xlsx"
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    for occ_name, group in df_it.groupby('occupation'):
        safe_sheet_name = occ_name[:31]  # batas maksimal nama sheet di Excel
        group.to_excel(writer, sheet_name=safe_sheet_name, index=False)

print(f"File berhasil dibuat: {output_path}")
