import pandas as pd
from rapidfuzz import fuzz, process

# === Load Data ===
df = pd.read_csv("jora_sharded.csv")

# === Daftar Occupation ANZSCO (IT) ===
occupations = [
    "135111 Chief Information Officer",
    "135112 ICT Project Manager",
    "135199 ICT Managers nec",
    "223211 ICT Trainer",
    "224114 Data Analyst",
    "224115 Data Scientist",
    "224999 Information and Organisation Professionals nec",
    "261111 ICT Business Analyst",
    "261112 Systems Analyst",
    "261211 Multimedia Specialist",
    "261212 Web Developer",
    "261311 Analyst Programmer",
    "261312 Developer Programmer",
    "261313 Software Engineer",
    "261314 Software Tester",
    "261315 Cyber Security Engineer",
    "261316 DevOps Engineer",
    "261317 Penetration Tester",
    "261399 Software & Applications Programmer nec",
    "262111 Database Administrator",
    "262112 ICT Security Specialist",
    "262113 Systems Administrator",
    "262114 Cyber Governance, Risk & Compliance Specialist",
    "262115 Cyber Security Advice & Assessment Specialist",
    "262116 Cyber Security Analyst",
    "262117 Cyber Security Architect",
    "262118 Cyber Security Operations Coordinator",
    "263111 Computer Network & Systems Engineer",
    "263112 Network Administrator",
    "263113 Network Analyst",
    "263211 ICT Quality Assurance Engineer",
    "263212 ICT Support Engineer",
    "263213 ICT Systems Test Engineer",
    "263299 ICT Support & Test Engineers nec",
    "313113 Web Administrator"
]

# === Alias mapping (lebih luas & umum) ===
alias_mapping = {
    # Management & Leadership
    "cio": "135111 Chief Information Officer",
    "chief information officer": "135111 Chief Information Officer",
    "project manager": "135112 ICT Project Manager",
    "pm": "135112 ICT Project Manager",
    "ict manager": "135199 ICT Managers nec",
    "it manager": "135199 ICT Managers nec",
    "manager": "135199 ICT Managers nec",

    # Trainers & Data
    "trainer": "223211 ICT Trainer",
    "ict trainer": "223211 ICT Trainer",
    "data analyst": "224114 Data Analyst",
    "analyst data": "224114 Data Analyst",
    "da": "224114 Data Analyst",
    "data scientist": "224115 Data Scientist",
    "ds": "224115 Data Scientist",
    "information professional": "224999 Information and Organisation Professionals nec",

    # Business & System Analysis
    "ba": "261111 ICT Business Analyst",
    "business analyst": "261111 ICT Business Analyst",
    "ict business analyst": "261111 ICT Business Analyst",
    "systems analyst": "261112 Systems Analyst",
    "system analyst": "261112 Systems Analyst",
    "analyst": "261112 Systems Analyst",

    # Multimedia & Web
    "multimedia": "261211 Multimedia Specialist",
    "multimedia specialist": "261211 Multimedia Specialist",
    "frontend": "261212 Web Developer",
    "backend": "261212 Web Developer",
    "fullstack": "261212 Web Developer",
    "full-stack": "261212 Web Developer",
    "web": "261212 Web Developer",
    "web developer": "261212 Web Developer",

    # Programming & Software
    "analyst programmer": "261311 Analyst Programmer",
    "developer": "261312 Developer Programmer",
    "programmer": "261312 Developer Programmer",
    "dev": "261312 Developer Programmer",
    "software engineer": "261313 Software Engineer",
    "software": "261313 Software Engineer",
    "engineer": "261313 Software Engineer",
    "tester": "261314 Software Tester",
    "qa tester": "261314 Software Tester",
    "qa": "261314 Software Tester",
    "quality assurance": "261314 Software Tester",
    "cyber security engineer": "261315 Cyber Security Engineer",
    "devops": "261316 DevOps Engineer",
    "devops engineer": "261316 DevOps Engineer",
    "sre": "261316 DevOps Engineer",
    "site reliability engineer": "261316 DevOps Engineer",
    "penetration tester": "261317 Penetration Tester",
    "pentest": "261317 Penetration Tester",
    "programmer nec": "261399 Software & Applications Programmer nec",
    "software programmer nec": "261399 Software & Applications Programmer nec",

    # Database & Security
    "database": "262111 Database Administrator",
    "db": "262111 Database Administrator",
    "dba": "262111 Database Administrator",
    "security": "262112 ICT Security Specialist",
    "ict security": "262112 ICT Security Specialist",
    "cybersecurity": "262112 ICT Security Specialist",
    "sysadmin": "262113 Systems Administrator",
    "system administrator": "262113 Systems Administrator",
    "governance": "262114 Cyber Governance, Risk & Compliance Specialist",
    "grc": "262114 Cyber Governance, Risk & Compliance Specialist",
    "risk compliance": "262114 Cyber Governance, Risk & Compliance Specialist",
    "cyber advice": "262115 Cyber Security Advice & Assessment Specialist",
    "security assessment": "262115 Cyber Security Advice & Assessment Specialist",
    "cyber analyst": "262116 Cyber Security Analyst",
    "security analyst": "262116 Cyber Security Analyst",
    "soc analyst": "262116 Cyber Security Analyst",
    "cyber architect": "262117 Cyber Security Architect",
    "security architect": "262117 Cyber Security Architect",
    "soc": "262118 Cyber Security Operations Coordinator",
    "secops": "262118 Cyber Security Operations Coordinator",
    "security operations": "262118 Cyber Security Operations Coordinator",

    # Network & Infrastructure
    "network": "263112 Network Administrator",
    "network admin": "263112 Network Administrator",
    "netadmin": "263112 Network Administrator",
    "network analyst": "263113 Network Analyst",
    "computer network engineer": "263111 Computer Network & Systems Engineer",
    "network engineer": "263111 Computer Network & Systems Engineer",

    # QA, Support & Testing
    "ict qa": "263211 ICT Quality Assurance Engineer",
    "qa engineer": "263211 ICT Quality Assurance Engineer",
    "quality assurance engineer": "263211 ICT Quality Assurance Engineer",
    "ict support": "263212 ICT Support Engineer",
    "support engineer": "263212 ICT Support Engineer",
    "helpdesk": "263212 ICT Support Engineer",
    "system test": "263213 ICT Systems Test Engineer",
    "systems test engineer": "263213 ICT Systems Test Engineer",
    "service desk": "263299 ICT Support & Test Engineers nec",
    "desktop support": "263299 ICT Support & Test Engineers nec",
    "support": "263299 ICT Support & Test Engineers nec",

    # Web Administration
    "web admin": "313113 Web Administrator",
    "webmaster": "313113 Web Administrator",
    "site admin": "313113 Web Administrator",
}

# === Fungsi untuk klasifikasi job title ===
def classify_job(title):
    if pd.isna(title):
        return "Unclassified"

    title_lower = title.lower()

    # 1. Cek alias mapping dulu
    for keyword, occ in alias_mapping.items():
        if keyword in title_lower:
            return occ

    # 2. Fuzzy matching ke occupation list
    best_match, score, _ = process.extractOne(
        title_lower, occupations, scorer=fuzz.token_sort_ratio
    )
    if score >= 25:  # threshold diturunkan
        return best_match

    return "Unclassified"

# === Proses klasifikasi ===
df["occupation_matched"] = df["title"].apply(classify_job)

# === Simpan ke Excel dengan sheet per occupation ===
output_file = "Job Dataset IT Ocuppation (Jora).xlsx"
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    for occ, group in df.groupby("occupation_matched"):
        group.to_excel(writer, sheet_name=occ[:30], index=False)  # sheet max 31 char

print(f"Saved to {output_file}")

# === Hitung total rows biar bisa dicek ===
print("Total data asli:", len(df))
print("Total setelah klasifikasi:", sum(len(g) for _, g in df.groupby("occupation_matched")))
