"""
Mapeamento de países para códigos de idioma ISO 639-1 + região.
Fonte: idiomas oficiais e principais de cada país.
"""

LANG_MAP = {
    # A
    "Afghanistan": "fa-AF",           # Persa/Dari
    "Albania": "sq-AL",               # Albanês
    "Algeria": "ar-DZ",               # Árabe
    "Andorra": "ca-AD",               # Catalão
    "Angola": "pt-AO",                # Português
    "Antigua and Barbuda": "en-AG",   # Inglês
    "Argentina": "es-AR",             # Espanhol
    "Armenia": "hy-AM",               # Armênio
    "Australia": "en-AU",             # Inglês
    "Austria": "de-AT",               # Alemão
    "Azerbaijan": "az-AZ",            # Azeri
    # B
    "Bahamas": "en-BS",               # Inglês
    "Bahrain": "ar-BH",               # Árabe
    "Bangladesh": "bn-BD",            # Bengali
    "Barbados": "en-BB",              # Inglês
    "Belarus": "be-BY",               # Belarusso
    "Belgium": "nl-BE",               # Holandês/Francês/Alemão
    "Belize": "en-BZ",                # Inglês
    "Benin": "fr-BJ",                 # Francês
    "Bhutan": "dz-BT",                # Dzongkha
    "Bolivia": "es-BO",               # Espanhol/Quechua/Aymara
    "Bosnia and Herzegovina": "bs-BA", # Bósnio
    "Botswana": "en-BW",              # Inglês/Tswana
    "Brazil": "pt-BR",                # Português
    "Brunei": "ms-BN",                # Malaio
    "Bulgaria": "bg-BG",              # Búlgaro
    "Burkina Faso": "fr-BF",          # Francês
    "Burundi": "rn-BI",               # Kirundi
    # C
    "Cambodia": "km-KH",              # Khmer
    "Cameroon": "fr-CM",              # Francês/Inglês
    "Canada": "en-CA",                # Inglês/Francês
    "Cape Verde": "pt-CV",            # Português
    "Central African Republic": "fr-CF", # Francês
    "Chad": "fr-TD",                  # Francês/Árabe
    "Chile": "es-CL",                 # Espanhol
    "China": "zh-CN",                 # Chinês Mandarin
    "Colombia": "es-CO",              # Espanhol
    "Comoros": "ar-KM",               # Árabe/Comoriano
    "Congo": "fr-CG",                 # Francês
    "Congo The Democratic Republic Of The": "fr-CD", # Francês
    "Costa Rica": "es-CR",            # Espanhol
    "Croatia": "hr-HR",               # Croata
    "Cuba": "es-CU",                  # Espanhol
    "Cyprus": "el-CY",                # Grego/Turco
    "Czech Republic": "cs-CZ",        # Tcheco
    # D
    "Denmark": "da-DK",               # Dinamarquês
    "Djibouti": "fr-DJ",              # Francês/Árabe
    "Dominica": "en-DM",              # Inglês
    "Dominican Republic": "es-DO",    # Espanhol
    # E
    "Ecuador": "es-EC",               # Espanhol
    "Egypt": "ar-EG",                 # Árabe
    "El Salvador": "es-SV",           # Espanhol
    "Equatorial Guinea": "es-GQ",     # Espanhol/Francês/Português
    "Eritrea": "ti-ER",               # Tigrinya
    "Estonia": "et-EE",               # Estoniano
    "Ethiopia": "am-ET",              # Amárico
    # F
    "Fiji": "en-FJ",                  # Inglês/Fijiano
    "Finland": "fi-FI",               # Finlandês/Sueco
    "France": "fr-FR",                # Francês
    # G
    "Gabon": "fr-GA",                 # Francês
    "Gambia": "en-GM",                # Inglês
    "Georgia": "ka-GE",               # Georgiano
    "Germany": "de-DE",               # Alemão
    "Ghana": "en-GH",                 # Inglês
    "Greece": "el-GR",                # Grego
    "Grenada": "en-GD",               # Inglês
    "Guatemala": "es-GT",             # Espanhol
    "Guinea": "fr-GN",                # Francês
    "Guinea-Bissau": "pt-GW",         # Português
    "Guyana": "en-GY",                # Inglês
    # H
    "Haiti": "ht-HT",                 # Haitiano/Francês
    "Honduras": "es-HN",              # Espanhol
    "Hungary": "hu-HU",               # Húngaro
    # I
    "Iceland": "is-IS",               # Islandês
    "India": "hi-IN",                 # Hindi/Inglês
    "Indonesia": "id-ID",             # Indonésio
    "Iran": "fa-IR",                  # Persa
    "Iraq": "ar-IQ",                  # Árabe/Kurdo
    "Ireland": "en-IE",               # Inglês/Irlandês
    "Israel": "he-IL",                # Hebraico/Árabe
    "Italy": "it-IT",                 # Italiano
    "Ivory Coast": "fr-CI",           # Francês
    # J
    "Jamaica": "en-JM",               # Inglês
    "Japan": "ja-JP",                 # Japonês
    "Jordan": "ar-JO",                # Árabe
    # K
    "Kazakhstan": "kk-KZ",            # Cazaque/Russo
    "Kenya": "sw-KE",                 # Swahili/Inglês
    "Kiribati": "en-KI",              # Inglês
    "Kuwait": "ar-KW",                # Árabe
    "Kyrgyzstan": "ky-KG",            # Quirguiz/Russo
    # L
    "Laos": "lo-LA",                  # Lao
    "Latvia": "lv-LV",                # Letão
    "Lebanon": "ar-LB",               # Árabe
    "Lesotho": "en-LS",               # Inglês/Sotho
    "Liberia": "en-LR",               # Inglês
    "Libya": "ar-LY",                 # Árabe
    "Liechtenstein": "de-LI",         # Alemão
    "Lithuania": "lt-LT",             # Lituano
    "Luxembourg": "lb-LU",            # Luxemburguês/Francês/Alemão
    # M
    "Madagascar": "mg-MG",            # Malgaxe/Francês
    "Malawi": "en-MW",                # Inglês/Chichewa
    "Malaysia": "ms-MY",              # Malaio
    "Maldives": "dv-MV",              # Divehi
    "Mali": "fr-ML",                  # Francês
    "Malta": "mt-MT",                 # Maltês/Inglês
    "Marshall Islands": "en-MH",      # Inglês/Marshallese
    "Mauritania": "ar-MR",            # Árabe
    "Mauritius": "en-MU",             # Inglês
    "Mexico": "es-MX",                # Espanhol
    "Micronesia": "en-FM",            # Inglês
    "Moldova": "ro-MD",               # Romeno
    "Monaco": "fr-MC",                # Francês
    "Mongolia": "mn-MN",              # Mongol
    "Montenegro": "sr-ME",            # Montenegrino/Sérvio
    "Morocco": "ar-MA",               # Árabe/Berber
    "Mozambique": "pt-MZ",            # Português
    "Myanmar": "my-MM",               # Birmanês
    # N
    "Namibia": "en-NA",               # Inglês
    "Nauru": "na-NR",                 # Nauruano/Inglês
    "Nepal": "ne-NP",                 # Nepalês
    "Netherlands": "nl-NL",           # Holandês
    "New Zealand": "en-NZ",           # Inglês/Maori
    "Nicaragua": "es-NI",             # Espanhol
    "Niger": "fr-NE",                 # Francês
    "Nigeria": "en-NG",               # Inglês
    "North Korea": "ko-KP",           # Coreano
    "North Macedonia": "mk-MK",       # Macedônio
    "Norway": "nb-NO",                # Norueguês Bokmål
    # O
    "Oman": "ar-OM",                  # Árabe
    # P
    "Pakistan": "ur-PK",              # Urdu/Inglês
    "Palau": "en-PW",                 # Inglês/Palauano
    "Palestine": "ar-PS",             # Árabe
    "Panama": "es-PA",                # Espanhol
    "Papua New Guinea": "en-PG",      # Inglês/Tok Pisin/Hiri Motu
    "Paraguay": "es-PY",              # Espanhol/Guarani
    "Peru": "es-PE",                  # Espanhol/Quechua/Aymara
    "Philippines": "fil-PH",          # Filipino/Inglês
    "Poland": "pl-PL",                # Polonês
    "Portugal": "pt-PT",              # Português
    # Q
    "Qatar": "ar-QA",                 # Árabe
    # R
    "Romania": "ro-RO",               # Romeno
    "Russia": "ru-RU",                # Russo
    "Rwanda": "rw-RW",                # Kinyarwanda/Francês/Inglês
    # S
    "Saint Kitts And Nevis": "en-KN", # Inglês
    "Saint Lucia": "en-LC",           # Inglês
    "Saint Vincent And The Grenadines": "en-VC", # Inglês
    "Samoa": "sm-WS",                 # Samoano/Inglês
    "San Marino": "it-SM",            # Italiano
    "Sao Tome and Principe": "pt-ST", # Português
    "Saudi Arabia": "ar-SA",          # Árabe
    "Senegal": "fr-SN",               # Francês
    "Serbia": "sr-RS",                # Sérvio
    "Seychelles": "en-SC",            # Inglês/Seychelles Crioulo/Francês
    "Sierra Leone": "en-SL",          # Inglês
    "Singapore": "en-SG",             # Inglês/Malaio/Mandarim/Tamil
    "Slovakia": "sk-SK",              # Eslovaco
    "Slovenia": "sl-SI",              # Esloveno
    "Solomon Islands": "en-SB",       # Inglês
    "Somalia": "so-SO",               # Somali/Árabe
    "South Africa": "af-ZA",           # Africaans/Zulu/Xhosa/etc + Inglês
    "South Korea": "ko-KR",           # Coreano
    "South Sudan": "en-SS",           # Inglês
    "Spain": "es-ES",                 # Espanhol
    "Sri Lanka": "si-LK",             # Cingalês/Tamil
    "Sudan": "ar-SD",                 # Árabe/Inglês
    "Suriname": "nl-SR",              # Holandês
    "Sweden": "sv-SE",                # Sueco
    "Switzerland": "de-CH",           # Alemão/Francês/Italiano/Romanche
    "Syria": "ar-SY",                 # Árabe
    # T
    "Tajikistan": "tg-TJ",            # Tadjique
    "Tanzania": "sw-TZ",              # Swahili/Inglês
    "Thailand": "th-TH",              # Tailandês
    "Timor-Leste": "pt-TL",           # Português/Tetum
    "Togo": "fr-TG",                  # Francês
    "Tonga": "to-TO",                 # Tonga/Inglês
    "Trinidad and Tobago": "en-TT",   # Inglês
    "Tunisia": "ar-TN",               # Árabe
    "Turkey": "tr-TR",                # Turco
    "Turkmenistan": "tk-TM",          # Turcomeno
    "Tuvalu": "en-TV",                # Tuvaluano/Inglês
    # U
    "Uganda": "en-UG",                # Inglês/Swahili
    "Ukraine": "uk-UA",               # Ucraniano
    "United Arab Emirates": "ar-AE",  # Árabe
    "United Kingdom": "en-GB",        # Inglês
    "United States": "en-US",         # Inglês
    "Uruguay": "es-UY",               # Espanhol
    "Uzbekistan": "uz-UZ",            # Uzbeque
    # V
    "Vanuatu": "bi-VU",               # Bislama/Inglês/Francês
    "Vatican City": "it-VA",          # Italiano/Latim
    "Venezuela": "es-VE",             # Espanhol
    "Vietnam": "vi-VN",               # Vietnamita
    # Y
    "Yemen": "ar-YE",                 # Árabe
    # Z
    "Zambia": "en-ZM",                # Inglês
    "Zimbabwe": "en-ZW",              # Inglês/Shona/Ndebele
}


def get_language(country_name):
    """
    Retorna o código de idioma para um país.
    Se o país não estiver no mapeamento, retorna 'en' como padrão.
    """
    if not country_name:
        return "en"
    return LANG_MAP.get(country_name.strip(), "en")


def get_language_name(country_name):
    """
    Retorna apenas o código do idioma (ex: 'pt', 'en') sem a região.
    """
    lang = get_language(country_name)
    return lang.split('-')[0] if lang else "en"
