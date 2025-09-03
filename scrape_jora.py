# scrape_jora_sharded.py
import time, random, re, json
from pathlib import Path
from urllib.parse import urlencode, quote_plus, urljoin, urlparse
import html as ihtml
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ================== KONFIGURASI ==================
KEYWORDS = [
    "Chief Information Officer",
    "ICT Project Manager",
    "ICT Managers nec",
    "ICT Trainer",
    "Data Analyst",
    "Data Scientist",
    "Information and Organisation Professionals nec",
    "ICT Business Analyst",
    "Systems Analyst",
    "Multimedia Specialist",
    "Web Developer",
    "Analyst Programmer",
    "Developer Programmer",
    "Software Engineer",
    "Software Tester",
    "Cyber Security Engineer",
    "DevOps Engineer",
    "Penetration Tester",
    "Software & Applications Programmer nec",
    "Database Administrator",
    "ICT Security Specialist",
    "Systems Administrator",
    "Cyber Governance, Risk & Compliance Specialist",
    "Cyber Security Advice & Assessment Specialist",
    "Cyber Security Analyst",
    "Cyber Security Architect",
    "Cyber Security Operations Coordinator",
    "Computer Network & Systems Engineer",
    "Network Administrator",
    "Network Analyst",
    "ICT Quality Assurance Engineer",
    "ICT Support Engineer",
    "ICT Systems Test Engineer",
    "ICT Support & Test Engineers (nec)",
    "Web Administrator"
]

STATES = [
    "NSW","New South Wales",
    "VIC", "Victoria",
    "QLD", "Queensland",
    "SA", "South Australia",
    "WA", "Western Australia",
    "TAS", "Tasmania",
    "ACT", "Australian Capital Territory",
    "NT", "Northern Territory"
]
MAX_PAGES_PER_SHARD = 10     
DELAY_LIST_S = (0.7, 1.4)
DELAY_JOB_S  = (0.6, 1.2)
TIMEOUT = 30
SAVE_EVERY_N = 500             
OUT_CSV = "jora_sharded_occ.csv"
OUT_XLSX = "jora_sharded_occ.xlsx"
# =================================================

SESSION = requests.Session()
SESSION.headers.update({
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://au.jora.com/",
})

SESSION.cookies.set("consent", "yes", domain="au.jora.com")
SESSION.cookies.set("euconsent-v2", "YES", domain="au.jora.com")
SESSION.cookies.set("jora_country", "AU", domain="au.jora.com")
SESSION.cookies.set("jora_lang", "en", domain="au.jora.com")

def clean(s:str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def get_soup(url:str):
    r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def build_list_urls(q:str, loc:str, page:int):
    url1 = "https://au.jora.com/j?" + urlencode({
        "q": q, "l": loc, "sp": "homepage",
        "trigger_source": "homepage", "p": page
    })
    q_slug = re.sub(r"\s+", "-", q.strip().lower())
    l_slug = re.sub(r"\s+", "-", loc.strip().lower())
    url2 = f"https://au.jora.com/jobs-{quote_plus(q_slug)}-in-{quote_plus(l_slug)}?p={page}"
    url3 = f"https://au.jora.com/{quote_plus(q)}-jobs-in-{quote_plus(loc)}?p={page}"
    return [url1, url2, url3]

def find_cards(soup: BeautifulSoup):
    cards = soup.select('[data-testid="job-item"], article[data-testid="job-item"]')
    if not cards:
        cards = soup.select('article[id^="job-"], article.job, .job-card, .job-item, .result')
    if not cards:
        # fallback sangat generik
        for h in soup.select("h2:has(a), h3:has(a)"):
            a = h.select_one("a")
            if a and "/job/" in a.get("href",""):
                cards.append(h.parent or h)
    return cards

def pick_text(node, selectors):
    for css in selectors:
        el = node.select_one(css)
        if el:
            return clean(el.get_text(" ", strip=True))
    return ""

def parse_card(card):
    a = (card.select_one('a[data-testid="job-title"]')
         or card.select_one('a[href*="/job/"]')
         or card.select_one('h2 a, h3 a'))
    title = clean(a.get_text()) if a else ""
    href = a["href"] if a and a.has_attr("href") else ""
    job_url = href if href.startswith("http") else urljoin("https://au.jora.com", href)

    company = pick_text(card, [
        '[data-testid="job-company"]','[data-automation*="company"]',
        '.job-company','.company','.-company'
    ])
    location = pick_text(card, [
        '[data-testid="job-location"]','[data-automation*="location"]',
        '.-location','.location','.job-location'
    ])
    meta_text = clean(card.get_text(" | ", strip=True))
    job_type = ""
    for jt in ["Full time","Part time","Contract","Permanent","Casual","Temporary",
               "Traineeship","Internship","Fixed term"]:
        if re.search(rf"\b{re.escape(jt)}\b", meta_text, re.I):
            job_type = jt; break

    salary = pick_text(card, [
        '[data-testid="job-salary"]','[data-automation*="salary"]',
        '.salary','.-salary'
    ])
    if not salary:
        m = re.search(r"\$\s?[\d,]+(?:\s?-\s?\$\s?[\d,]+)?(?:\s*(?:per|/)\s*(?:hour|annum|year|day|month))?", meta_text, re.I)
        if m: salary = clean(m.group(0))

    # --- NEW: job-abstract di kartu sebagai fallback description ---
    abstract_items = []
    # list item
    for li in card.select('[data-testid="job-abstract"] li, [data-automation*="job-abstract"] li, .job-abstract li'):
        t = clean(li.get_text(" ", strip=True))
        if t:
            abstract_items.append(t)
    # paragraf
    if not abstract_items:
        for p in card.select('[data-testid="job-abstract"], [data-automation*="job-abstract"], .job-abstract p'):
            t = clean(p.get_text(" ", strip=True))
            if t:
                abstract_items.append(t)
    abstract = " • ".join(abstract_items[:12])  # batasi supaya ringkas

    return {"title": title, "company": company, "location": location,
            "job_type": job_type, "salary": salary, "job_url": job_url,
            "abstract": abstract}  # <-- field sementara (tidak diekspor)

def parse_job_jsonld(soup: BeautifulSoup):
    def norm_desc(html_text: str) -> str:
        if not html_text:
            return ""
        txt = ihtml.unescape(str(html_text))
        txt = re.sub(r"(?i)<br\s*/?>", "\n", txt)
        txt = re.sub(r"<[^>]+>", " ", txt)
        return clean(txt)

    def candidate(obj):
        if not isinstance(obj, dict):
            return None
        t = obj.get("@type")
        if t == "JobPosting" or (isinstance(t, list) and "JobPosting" in t):
            out = {"salary":"", "description":"", "location":""}
            base = obj.get("baseSalary") or {}
            if isinstance(base, dict):
                val = base.get("value"); unit = ""
                if isinstance(val, dict):
                    mn = val.get("minValue"); mx = val.get("maxValue"); unit = (val.get("unitText") or "").lower()
                    if mn and mx: out["salary"] = f"${int(float(mn)):,} - ${int(float(mx)):,} per {unit}"
                    elif mn:      out["salary"] = f"from ${int(float(mn)):,} per {unit}"
                    elif mx:      out["salary"] = f"to ${int(float(mx)):,} per {unit}"
                elif isinstance(val, (int,float)):
                    out["salary"] = f"${int(val):,}"
            out["salary"] = out["salary"] or clean(obj.get("salary",""))
            out["description"] = norm_desc(obj.get("description",""))

            jl = obj.get("jobLocation")
            if isinstance(jl, dict):
                addr = jl.get("address",{})
                out["location"] = clean(", ".join([addr.get("addressLocality",""),
                                                   addr.get("addressRegion",""),
                                                   addr.get("addressCountry","")]))
            elif isinstance(jl, list) and jl:
                addr = (jl[0] or {}).get("address",{})
                out["location"] = clean(", ".join([addr.get("addressLocality",""),
                                                   addr.get("addressRegion",""),
                                                   addr.get("addressCountry","")]))
            return out

    # 1) JSON-LD normal / @graph
    for sc in soup.select('script[type="application/ld+json"]'):
        payload = sc.string or "".join(sc.strings)
        payload = (payload or "").strip().rstrip(";")
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except Exception:
            try:
                data = json.loads(ihtml.unescape(payload))
            except Exception:
                continue

        items = data["@graph"] if isinstance(data, dict) and isinstance(data.get("@graph"), list) else (data if isinstance(data, list) else [data])
        for it in items:
            got = candidate(it)
            if got and (got["salary"] or got["description"] or got["location"]):
                return got

    # 2) NEXT.JS __NEXT_DATA__ (sering dipakai)
    nd = soup.find("script", id="__NEXT_DATA__")
    if nd and nd.string:
        try:
            data = json.loads(nd.string)
            def deep_find_desc(obj):
                if isinstance(obj, dict):
                    for k,v in obj.items():
                        if isinstance(v, (dict,list)):
                            r = deep_find_desc(v)
                            if r: return r
                        else:
                            if isinstance(v, str) and len(v) > 150 and any(tok in v.lower() for tok in ["responsibilit","requirement","we are", "about the role","about you","position"]):
                                return v
                elif isinstance(obj, list):
                    for v in obj:
                        r = deep_find_desc(v)
                        if r: return r
                return ""
            desc_raw = deep_find_desc(data)
            if desc_raw:
                return {"salary":"", "description": norm_desc(desc_raw), "location":""}
        except Exception:
            pass

    return {"salary":"", "description":"", "location":""}

def parse_job_detail(job_url: str):
    def find_external_url(soup: BeautifulSoup, current_url: str) -> str:
        can = soup.find("link", rel="canonical")
        if can and can.get("href"):
            href = can["href"].strip()
            if urlparse(href).netloc and "jora.com" not in urlparse(href).netloc:
                return href
        ogu = soup.find("meta", attrs={"property":"og:url"})
        if ogu and ogu.get("content"):
            href = ogu["content"].strip()
            if urlparse(href).netloc and "jora.com" not in urlparse(href).netloc:
                return href
        for a in soup.select('a[href]'):
            href = (a.get("href") or "").strip()
            if href.lower().startswith(("http://","https://")) and "jora.com" not in href.lower():
                if not href.lower().startswith(("mailto:","tel:")):
                    return href
        return ""

    def extract_desc_from_external(url: str) -> str:
        try:
            s = get_soup(url)
        except Exception:
            return ""
        # meta description dulu
        meta = s.find("meta", attrs={"name":"description"})
        if meta and meta.get("content"):
            txt = clean(meta["content"])
            if len(txt) > 60:
                return txt
        ogd = s.find("meta", attrs={"property":"og:description"})
        if ogd and ogd.get("content"):
            txt = clean(ogd["content"])
            if len(txt) > 60:
                return txt
        # cari blok teks terpanjang
        candidates = []
        for css in [
            '[data-testid="jobAdDetails"]',
            "article", "main", "section[role=main]", "div[role=main]",
            "[class*='description']", "[id*='description']",
            "[class*='job']", "[id*='job']",
            "section", "div"
        ]:
            for el in s.select(css):
                t = clean(el.get_text(" ", strip=True))
                if len(t) > 200:
                    candidates.append((len(t), t))
        if candidates:
            return max(candidates, key=lambda x: x[0])[1][:8000]
        return ""

    try:
        soup = get_soup(job_url)
    except Exception:
        return {"salary":"", "description":"", "location":""}

    out = parse_job_jsonld(soup)

    # Fallback SALARY (DOM)
    if not out["salary"]:
        out["salary"] = pick_text(soup, [
            '[data-testid="job-salary"]','[data-automation*="salary"]',
            '.salary','.-salary','.job-attributes li:contains("$")'
        ])

    # Fallback DESCRIPTION (DOM)
    if not out["description"] or len(out["description"]):
        for css in [
            '[data-testid="jobad-details"]',
            '[data-automation*="jobAdDetails"]',
            '[data-qa*="job-description"]',
            '.jobad','.jobad-content','.job-description',
            'main[role="main"]','main',
            'article[role="article"]','article',
            '#jobad','.content'
        ]:
            el = soup.select_one(css)
            if el:
                for s in el.select("script,style,nav,header,footer"): s.decompose()
                text = clean(el.get_text(" ", strip=True))
                if len(text) > len(out["description"]):
                    out["description"] = text
                    break

    # Fallback LOCATION (DOM)
    if not out["location"]:
        out["location"] = pick_text(soup, [
            '[data-testid="job-location"]','[data-automation*="location"]',
            '.-location','.location','.job-location','li[aria-label*="location"]'
        ])

    # **Follow halaman eksternal** jika deskripsi masih minim
    if not out["description"]:
        ext = find_external_url(soup, job_url)
        if ext:
            ext_desc = extract_desc_from_external(ext)
            if len(ext_desc) > len(out["description"]):
                out["description"] = ext_desc

    # SANGAT TERAKHIR: jika masih kosong, simpan satu HTML untuk debug
    if not getattr(parse_job_detail, "_dumped", False) and (not out["description"] or len(out["description"]) < 40):
        try:
            html_debug = requests.get(job_url, headers=SESSION.headers, timeout=TIMEOUT).text
            Path("debug_job_one.html").write_text(html_debug, encoding="utf-8")
            parse_job_detail._dumped = True
        except Exception:
            pass

    return out

def fetch_list_page(q:str, loc:str, page:int):
    for url in build_list_urls(q, loc, page):
        try:
            soup = get_soup(url)
            cards = find_cards(soup)
            if cards:
                return cards
        except Exception:
            continue
    return []

def run():
    seen = set()  # job_url for dedup
    rows = []
    for kw in KEYWORDS:
        for st in STATES:
            shard = f"{kw} @ {st}"
            print(f"=== Shard: {shard}")
            for p in range(1, MAX_PAGES_PER_SHARD+1):
                print(f"[{shard}] Page {p}")
                cards = fetch_list_page(kw, st, p)
                if not cards:
                    # berhenti kalau halaman ini kosong
                    break
                for card in cards:
                    base = parse_card(card)
                    if not (base["title"] and base["job_url"]):
                        continue
                    if base["job_url"] in seen:
                        continue
                    time.sleep(random.uniform(*DELAY_JOB_S))
                    detail = parse_job_detail(base["job_url"])
                    base["salary"] = base["salary"] or detail["salary"]
                    # --- NEW: pakai abstract sebagai fallback kalau detail kosong ---
                    base["description"] = detail["description"] or base.get("abstract","")
                    base["location"] = base["location"] or detail["location"]
                    seen.add(base["job_url"])
                    rows.append(base)

                    # flush berkala agar aman kalau terhenti
                    if len(rows) % SAVE_EVERY_N == 0:
                        df = pd.DataFrame(rows, columns=["title","company","location","job_type","salary","description","job_url"])
                        df.drop_duplicates(subset=["job_url"], inplace=True)
                        df = df[["title","company","location","job_type","salary","description"]]
                        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
                time.sleep(random.uniform(*DELAY_LIST_S))

    if not rows:
        print("Tidak ada data terkumpul.")
        return

    df = pd.DataFrame(rows, columns=["title","company","location","job_type","salary","description","job_url"])
    df.drop_duplicates(subset=["job_url"], inplace=True)
    df = df[["title","company","location","job_type","salary","description"]]
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Data")
        ws = w.sheets["Data"]
        widths = {"title":52,"company":28,"location":28,"job_type":16,"salary":24,"description":90}
        for i,col in enumerate(df.columns):
            ws.set_column(i, i, widths.get(col, 24))
    print(f"Selesai: {len(df)} baris unik → {OUT_CSV} & {OUT_XLSX}")

if __name__ == "__main__":
    run()
