"""
Documentos de identificação por país.
Inclui tipos de documentos para pessoas físicas e jurídicas.

Estrutura:
- type: sigla do documento (CPF, CNPJ, SSN, etc.)
- name: nome completo do documento
- person_type: 'individual' (pessoa física), 'company' (pessoa jurídica), 'both' (ambos)
- format: máscara de formatação (# = número, @ = letra, * = alfanumérico)
- regex: padrão de validação
- length: comprimento total (com ou sem formatação)
- numeric_only: True se apenas números, False se alfanumérico
- example: exemplo de documento válido
"""

COUNTRY_DOCUMENTS = {
    # A
    "AFG": [
        {"type": "Tazkira", "name": "Tazkira (National ID)", "person_type": "individual", "format": "############", "regex": r"^\d{12,13}$", "length": 13, "numeric_only": True, "example": "1234567890123"}
    ],
    "ARG": [
        {"type": "DNI", "name": "Documento Nacional de Identidad", "person_type": "individual", "format": "##.###.###", "regex": r"^\d{2}\.?\d{3}\.?\d{3}$", "length": 8, "numeric_only": True, "example": "12.345.678"},
        {"type": "CUIL", "name": "Código Único de Identificación Laboral", "person_type": "individual", "format": "##-########-#", "regex": r"^\d{2}-?\d{8}-?\d$", "length": 11, "numeric_only": True, "example": "20-12345678-9"},
        {"type": "CUIT", "name": "Código Único de Identificación Tributaria", "person_type": "company", "format": "##-########-#", "regex": r"^\d{2}-?\d{8}-?\d$", "length": 11, "numeric_only": True, "example": "30-12345678-9"}
    ],
    "AUS": [
        {"type": "TFN", "name": "Tax File Number", "person_type": "both", "format": "### ### ###", "regex": r"^\d{3}\s?\d{3}\s?\d{3}$", "length": 9, "numeric_only": True, "example": "123 456 789"},
        {"type": "ABN", "name": "Australian Business Number", "person_type": "company", "format": "## ### ### ###", "regex": r"^\d{2}\s?\d{3}\s?\d{3}\s?\d{3}$", "length": 11, "numeric_only": True, "example": "12 345 678 901"}
    ],
    "AUT": [
        {"type": "SVNR", "name": "Sozialversicherungsnummer", "person_type": "individual", "format": "#### ######", "regex": r"^\d{4}\s?\d{6}$", "length": 10, "numeric_only": True, "example": "1234 567890"}
    ],
    # B
    "BEL": [
        {"type": "NN", "name": "Nummer der Nationale", "person_type": "individual", "format": "##.##.##-###.##", "regex": r"^\d{2}\.?\d{2}\.?\d{2}-?\d{3}\.?\d{2}$", "length": 11, "numeric_only": True, "example": "12.34.56-789.01"},
        {"type": "KBO", "name": "Kruispuntbank van Ondernemingen", "person_type": "company", "format": "###.###.###", "regex": r"^\d{3}\.?\d{3}\.?\d{3}$", "length": 10, "numeric_only": True, "example": "123.456.789"}
    ],
    "BOL": [
        {"type": "CI", "name": "Cédula de Identidad", "person_type": "individual", "format": "########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"},
        {"type": "NIT", "name": "Número de Identificación Tributaria", "person_type": "company", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"}
    ],
    "BRA": [
        {"type": "CPF", "name": "Cadastro de Pessoas Físicas", "person_type": "individual", "format": "###.###.###-##", "regex": r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$", "length": 11, "numeric_only": True, "example": "123.456.789-01"},
        {"type": "CNPJ", "name": "Cadastro Nacional da Pessoa Jurídica", "person_type": "company", "format": "##.###.###/####-##", "regex": r"^\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}$", "length": 14, "numeric_only": True, "example": "12.345.678/0001-90"},
        {"type": "RG", "name": "Registro Geral", "person_type": "individual", "format": "##.###.###-#", "regex": r"^\d{2}\.?\d{3}\.?\d{3}-?\d$", "length": 9, "numeric_only": True, "example": "12.345.678-9"}
    ],
    "BGR": [
        {"type": "EGN", "name": "Единен граждански номер", "person_type": "individual", "format": "##########", "regex": r"^\d{10}$", "length": 10, "numeric_only": True, "example": "1234567890"}
    ],
    # C
    "CAN": [
        {"type": "SIN", "name": "Social Insurance Number", "person_type": "individual", "format": "### ### ###", "regex": r"^\d{3}\s?\d{3}\s?\d{3}$", "length": 9, "numeric_only": True, "example": "123 456 789"},
        {"type": "BN", "name": "Business Number", "person_type": "company", "format": "##########", "regex": r"^\d{9,15}$", "length": 9, "numeric_only": True, "example": "123456789"}
    ],
    "CHL": [
        {"type": "RUT", "name": "Rol Único Tributario", "person_type": "both", "format": "##.###.###-#", "regex": r"^\d{2}\.?\d{3}\.?\d{3}-?[\dkK]$", "length": 9, "numeric_only": False, "example": "12.345.678-9"}
    ],
    "CHN": [
        {"type": "ID Card", "name": "Resident Identity Card", "person_type": "individual", "format": "##################", "regex": r"^\d{17}[\dX]$", "length": 18, "numeric_only": False, "example": "11010519491231002X"},
        {"type": "USCC", "name": "Unified Social Credit Code", "person_type": "company", "format": "#################A", "regex": r"^[0-9A-Z]{18}$", "length": 18, "numeric_only": False, "example": "91110000100001234A"}
    ],
    "COL": [
        {"type": "CC", "name": "Cédula de Ciudadanía", "person_type": "individual", "format": "#.###.###.###", "regex": r"^\d{1,10}$", "length": 10, "numeric_only": True, "example": "1.234.567.890"},
        {"type": "CE", "name": "Cédula de Extranjería", "person_type": "individual", "format": "##########", "regex": r"^\d{6,10}$", "length": 10, "numeric_only": True, "example": "1234567890"},
        {"type": "NIT", "name": "Número de Identificación Tributaria", "person_type": "company", "format": "###.###.###-#", "regex": r"^\d{3}\.?\d{3}\.?\d{3}-?\d$", "length": 10, "numeric_only": True, "example": "123.456.789-0"}
    ],
    "CRI": [
        {"type": "Cédula", "name": "Cédula de Identidad", "person_type": "individual", "format": "#-###-######", "regex": r"^\d-\d{3}-\d{6}$", "length": 10, "numeric_only": True, "example": "1-234-567890"}
    ],
    "HRV": [
        {"type": "OIB", "name": "Osobni identifikacijski broj", "person_type": "both", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"}
    ],
    "CZE": [
        {"type": "RČ", "name": "Rodné číslo", "person_type": "individual", "format": "######/####", "regex": r"^\d{6}/?\d{4}$", "length": 10, "numeric_only": True, "example": "123456/7890"}
    ],
    # D
    "DNK": [
        {"type": "CPR", "name": "Det Centrale Personregister", "person_type": "individual", "format": "######-####", "regex": r"^\d{6}-?\d{4}$", "length": 10, "numeric_only": True, "example": "123456-7890"},
        {"type": "CVR", "name": "Centrale Virksomhedsregister", "person_type": "company", "format": "########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"}
    ],
    "DOM": [
        {"type": "Cédula", "name": "Cédula de Identidad y Electoral", "person_type": "individual", "format": "###-#######-#", "regex": r"^\d{3}-?\d{7}-?\d$", "length": 11, "numeric_only": True, "example": "123-4567890-1"}
    ],
    # E
    "ECU": [
        {"type": "CI", "name": "Cédula de Identidad", "person_type": "individual", "format": "##########", "regex": r"^\d{10}$", "length": 10, "numeric_only": True, "example": "1234567890"},
        {"type": "RUC", "name": "Registro Único de Contribuyentes", "person_type": "company", "format": "#############", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "1234567890001"}
    ],
    "EGY": [
        {"type": "RN", "name": "الرقم القومي", "person_type": "individual", "format": "############", "regex": r"^\d{14}$", "length": 14, "numeric_only": True, "example": "12345678901234"}
    ],
    "SLV": [
        {"type": "DUI", "name": "Documento Único de Identidad", "person_type": "individual", "format": "########-#", "regex": r"^\d{8}-?\d$", "length": 9, "numeric_only": True, "example": "12345678-9"},
        {"type": "NIT", "name": "Número de Identificación Tributaria", "person_type": "company", "format": "####-######-###-#", "regex": r"^\d{4}-?\d{6}-?\d{3}-?\d$", "length": 14, "numeric_only": True, "example": "1234-567890-123-4"}
    ],
    "EST": [
        {"type": "IK", "name": "Isikukood", "person_type": "individual", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"}
    ],
    # F
    "FIN": [
        {"type": "HETU", "name": "Henkilötunnus", "person_type": "individual", "format": "######-####", "regex": r"^\d{6}[+-A]\d{3}[\dA-FHJ-NPR-Y]$", "length": 11, "numeric_only": False, "example": "123456-7890"},
        {"type": "Y-tunnus", "name": "Yritys- ja yhteisötunnus", "person_type": "company", "format": "#######-#", "regex": r"^\d{7}-?\d$", "length": 8, "numeric_only": True, "example": "1234567-8"}
    ],
    "FRA": [
        {"type": "INSEE", "name": "Numéro de sécurité sociale", "person_type": "individual", "format": "# ## ## ## ### ### ##", "regex": r"^\d{15}$", "length": 15, "numeric_only": True, "example": "1 23 45 67 890 123 45"},
        {"type": "SIRET", "name": "Système d'identification du répertoire des établissements", "person_type": "company", "format": "### ### ### #####", "regex": r"^\d{3}\s?\d{3}\s?\d{3}\s?\d{5}$", "length": 14, "numeric_only": True, "example": "123 456 789 00012"}
    ],
    # G
    "DEU": [
        {"type": "Steuer-ID", "name": "Steuerliche Identifikationsnummer", "person_type": "individual", "format": "## ### ### ###", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12 345 678 901"},
        {"type": "St-Nr", "name": "Steuernummer", "person_type": "company", "format": "### ### ### ###", "regex": r"^\d{10,13}$", "length": 13, "numeric_only": True, "example": "123 456 789 012"}
    ],
    "GTM": [
        {"type": "CUI", "name": "Código Único de Identificación", "person_type": "individual", "format": "#############", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "1234567890123"},
        {"type": "NIT", "name": "Número de Identificación Tributaria", "person_type": "company", "format": "######-#", "regex": r"^\d{6}-?\d$", "length": 7, "numeric_only": True, "example": "123456-7"}
    ],
    "GRC": [
        {"type": "AMKA", "name": "Αριθμός Μητρώου Κοινωνικής Ασφάλισης", "person_type": "individual", "format": "## ## ## #####", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12 34 56 78901"},
        {"type": "AFM", "name": "Αριθμός Φορολογικού Μητρώου", "person_type": "both", "format": "### ### ###", "regex": r"^\d{9}$", "length": 9, "numeric_only": True, "example": "123 456 789"}
    ],
    # H
    "HND": [
        {"type": "ID", "name": "Documento Nacional de Identificación", "person_type": "individual", "format": "####-####-#####", "regex": r"^\d{4}-?\d{4}-?\d{5}$", "length": 13, "numeric_only": True, "example": "1234-5678-90123"}
    ],
    "HUN": [
        {"type": "SZIG", "name": "Személyi igazolvány szám", "person_type": "individual", "format": "###### ##", "regex": r"^\d{6}\s?[A-Z]{2}$", "length": 9, "numeric_only": False, "example": "123456 AB"},
        {"type": "TAJ", "name": "Társadalombiztosítási Azonosító Jet", "person_type": "individual", "format": "### ### ###", "regex": r"^\d{3}\s?\d{3}\s?\d{3}$", "length": 9, "numeric_only": True, "example": "123 456 789"},
        {"type": "Adószám", "name": "Adóazonosító jel", "person_type": "both", "format": "########-#-##", "regex": r"^\d{8}-?\d-?\d{2}$", "length": 11, "numeric_only": True, "example": "12345678-9-01"}
    ],
    # I
    "ISL": [
        {"type": "KT", "name": "Kennitala", "person_type": "both", "format": "######-####", "regex": r"^\d{6}-?\d{4}$", "length": 10, "numeric_only": True, "example": "123456-7890"}
    ],
    "IND": [
        {"type": "PAN", "name": "Permanent Account Number", "person_type": "both", "format": "AAAAA#####A", "regex": r"^[A-Z]{5}\d{4}[A-Z]$", "length": 10, "numeric_only": False, "example": "ABCDE1234F"},
        {"type": "Aadhaar", "name": "Aadhaar Number", "person_type": "individual", "format": "#### #### ####", "regex": r"^\d{4}\s?\d{4}\s?\d{4}$", "length": 12, "numeric_only": True, "example": "1234 5678 9012"},
        {"type": "CIN", "name": "Corporate Identification Number", "person_type": "company", "format": "L#####**#######", "regex": r"^[A-Z]\d{5}[A-Z0-9]{2}\d{7}$", "length": 15, "numeric_only": False, "example": "L12345AB1234567"}
    ],
    "IDN": [
        {"type": "NIK", "name": "Nomor Induk Kependudukan", "person_type": "individual", "format": "## ## ## ## ## ####", "regex": r"^\d{16}$", "length": 16, "numeric_only": True, "example": "12 34 56 78 90 1234"},
        {"type": "NPWP", "name": "Nomor Pokok Wajib Pajak", "person_type": "both", "format": "##.###.###.#-###.###", "regex": r"^\d{2}\.?\d{3}\.?\d{3}\.?\d-?\d{3}\.?\d{3}$", "length": 15, "numeric_only": True, "example": "12.345.678.9-012.345"}
    ],
    "IRL": [
        {"type": "PPSN", "name": "Personal Public Service Number", "person_type": "individual", "format": "#######T", "regex": r"^\d{7}[A-Z]{1,2}$", "length": 8, "numeric_only": False, "example": "1234567T"},
        {"type": "CRO", "name": "Company Registration Office", "person_type": "company", "format": "######", "regex": r"^\d{6}$", "length": 6, "numeric_only": True, "example": "123456"}
    ],
    "ISR": [
        {"type": "ID", "name": "תעודת זהות", "person_type": "individual", "format": "#########", "regex": r"^\d{9}$", "length": 9, "numeric_only": True, "example": "123456789"}
    ],
    "ITA": [
        {"type": "CF", "name": "Codice Fiscale", "person_type": "individual", "format": "AAAAAA##A##A###A", "regex": r"^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$", "length": 16, "numeric_only": False, "example": "RSSMRA85T10A562S"},
        {"type": "PIVA", "name": "Partita IVA", "person_type": "company", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"}
    ],
    # J
    "JPN": [
        {"type": "My Number", "name": "マイナンバー", "person_type": "individual", "format": "####-####-####", "regex": r"^\d{4}-?\d{4}-?\d{4}$", "length": 12, "numeric_only": True, "example": "1234-5678-9012"},
        {"type": "TIN", "name": "法人番号", "person_type": "company", "format": "############", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "1234567890123"}
    ],
    # K
    "KAZ": [
        {"type": "ЖСН", "name": "Жеке сәйкестендіру нөмірі", "person_type": "individual", "format": "############", "regex": r"^\d{12}$", "length": 12, "numeric_only": True, "example": "123456789012"},
        {"type": "БСН", "name": "Бизнес сәйкестендіру нөмірі", "person_type": "company", "format": "############", "regex": r"^\d{12}$", "length": 12, "numeric_only": True, "example": "123456789012"}
    ],
    "KOR": [
        {"type": "RRN", "name": "주민등록번호", "person_type": "individual", "format": "######-#######", "regex": r"^\d{6}-?\d{7}$", "length": 13, "numeric_only": True, "example": "123456-1234567"},
        {"type": "BRN", "name": "사업자등록번호", "person_type": "company", "format": "###-##-#####", "regex": r"^\d{3}-?\d{2}-?\d{5}$", "length": 10, "numeric_only": True, "example": "123-45-67890"}
    ],
    # L
    "LVA": [
        {"type": "PK", "name": "Personas kods", "person_type": "individual", "format": "######-#####", "regex": r"^\d{6}-?\d{5}$", "length": 11, "numeric_only": True, "example": "123456-78901"}
    ],
    "LTU": [
        {"type": "AK", "name": "Asmens kodas", "person_type": "individual", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"},
        {"type": "Įmonės kodas", "name": "Juridinių asmenų registras", "person_type": "company", "format": "########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"}
    ],
    "LUX": [
        {"type": "INSS", "name": "Numéro d'identification sécurité sociale", "person_type": "individual", "format": "## ## ## #### ##", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "12 34 56 7890 12"},
        {"type": "RCS", "name": "Registre de Commerce et des Sociétés", "person_type": "company", "format": "B######", "regex": r"^[A-Z]\d{6}$", "length": 7, "numeric_only": False, "example": "B123456"}
    ],
    # M
    "MEX": [
        {"type": "CURP", "name": "Clave Única de Registro de Población", "person_type": "individual", "format": "AAAA######AAAAAA##", "regex": r"^[A-Z]{4}\d{6}[A-Z]{6}\d{2}$", "length": 18, "numeric_only": False, "example": "AAAA010101AAAAAA01"},
        {"type": "RFC", "name": "Registro Federal de Contribuyentes", "person_type": "both", "format": "AAAA#########", "regex": r"^[A-Z]{4}\d{6}[A-Z0-9]{3}$", "length": 13, "numeric_only": False, "example": "AAAA010101ABC"}
    ],
    "MYS": [
        {"type": "NRIC", "name": "National Registration Identity Card", "person_type": "individual", "format": "######-##-####", "regex": r"^\d{6}-?\d{2}-?\d{4}$", "length": 12, "numeric_only": True, "example": "123456-12-3456"},
        {"type": "ROC", "name": "Registrar of Companies", "person_type": "company", "format": "######-A", "regex": r"^\d{6}-[A-Z]$", "length": 8, "numeric_only": False, "example": "123456-X"}
    ],
    # N
    "NIC": [
        {"type": "Cédula", "name": "Cédula de Identidad", "person_type": "individual", "format": "###-######-####A", "regex": r"^\d{3}-?\d{6}-?\d{4}[A-Z]$", "length": 14, "numeric_only": False, "example": "123-456789-0123A"}
    ],
    "NLD": [
        {"type": "BSN", "name": "Burgerservicenummer", "person_type": "individual", "format": "########", "regex": r"^\d{8}$", "length": 9, "numeric_only": True, "example": "123456789"},
        {"type": "KvK", "name": "Kamer van Koophandel", "person_type": "company", "format": "########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"}
    ],
    "NOR": [
        {"type": "Fødselsnummer", "name": "Fødselsnummer", "person_type": "individual", "format": "###### #####", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "123456 78901"},
        {"type": "Org.nr", "name": "Organisasjonsnummer", "person_type": "company", "format": "### ### ###", "regex": r"^\d{3}\s?\d{3}\s?\d{3}$", "length": 9, "numeric_only": True, "example": "123 456 789"}
    ],
    "NZL": [
        {"type": "IRD", "name": "Inland Revenue Department Number", "person_type": "both", "format": "###-###-###", "regex": r"^\d{3}-?\d{3}-?\d{3}$", "length": 9, "numeric_only": True, "example": "123-456-789"}
    ],
    # P
    "PAK": [
        {"type": "CNIC", "name": "Computerised National Identity Card", "person_type": "individual", "format": "#####-#######-#", "regex": r"^\d{5}-?\d{7}-?\d$", "length": 13, "numeric_only": True, "example": "12345-1234567-1"}
    ],
    "PAN": [
        {"type": "Cédula", "name": "Cédula de Identidad", "person_type": "individual", "format": "###-###-#####", "regex": r"^\d{3}-?\d{3}-?\d{5}$", "length": 11, "numeric_only": True, "example": "123-456-78901"}
    ],
    "PRY": [
        {"type": "CI", "name": "Cédula de Identidad", "person_type": "individual", "format": "#.###.###", "regex": r"^\d{1,7}$", "length": 7, "numeric_only": True, "example": "1.234.567"},
        {"type": "RUC", "name": "Registro Único de Contribuyentes", "person_type": "company", "format": "########-#", "regex": r"^\d{8}-?\d$", "length": 9, "numeric_only": True, "example": "12345678-9"}
    ],
    "PER": [
        {"type": "DNI", "name": "Documento Nacional de Identidad", "person_type": "individual", "format": "########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"},
        {"type": "RUC", "name": "Registro Único de Contribuyentes", "person_type": "company", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"}
    ],
    "PHL": [
        {"type": "UMID", "name": "Unified Multi-Purpose ID", "person_type": "individual", "format": "####-####-####-#", "regex": r"^\d{4}-?\d{4}-?\d{4}-?\d$", "length": 13, "numeric_only": True, "example": "1234-5678-9012-3"},
        {"type": "TIN", "name": "Taxpayer Identification Number", "person_type": "both", "format": "###-###-###-###", "regex": r"^\d{3}-?\d{3}-?\d{3}-?\d{3}$", "length": 12, "numeric_only": True, "example": "123-456-789-000"}
    ],
    "POL": [
        {"type": "PESEL", "name": "Polski system numeracji ewidencyjnej", "person_type": "individual", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"},
        {"type": "NIP", "name": "Numer Identyfikacji Podatkowej", "person_type": "both", "format": "###-##-##-###", "regex": r"^\d{3}-?\d{2}-?\d{2}-?\d{3}$", "length": 10, "numeric_only": True, "example": "123-45-67-890"},
        {"type": "REGON", "name": "Rejestr Gospodarki Narodowej", "person_type": "company", "format": "#########", "regex": r"^\d{9,14}$", "length": 9, "numeric_only": True, "example": "123456789"}
    ],
    "PRT": [
        {"type": "NIF", "name": "Número de Identificação Fiscal", "person_type": "individual", "format": "#########", "regex": r"^\d{9}$", "length": 9, "numeric_only": True, "example": "123456789"},
        {"type": "NIPC", "name": "Número de Identificação de Pessoa Coletiva", "person_type": "company", "format": "#########", "regex": r"^\d{9}$", "length": 9, "numeric_only": True, "example": "123456789"}
    ],
    # R
    "ROU": [
        {"type": "CNP", "name": "Cod Numeric Personal", "person_type": "individual", "format": "######", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "1234567890123"},
        {"type": "CUI", "name": "Cod Unic de Înregistrare", "person_type": "company", "format": "##########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"}
    ],
    "RUS": [
        {"type": "ИНН", "name": "Идентификационный номер налогоплательщика", "person_type": "both", "format": "############", "regex": r"^\d{10,12}$", "length": 12, "numeric_only": True, "example": "123456789012"}
    ],
    # S
    "SAU": [
        {"type": "Iqama", "name": "الإقامة", "person_type": "individual", "format": "##########", "regex": r"^\d{10}$", "length": 10, "numeric_only": True, "example": "1234567890"},
        {"type": "CR", "name": "Commercial Registration", "person_type": "company", "format": "##########", "regex": r"^\d{10}$", "length": 10, "numeric_only": True, "example": "1234567890"}
    ],
    "SRB": [
        {"type": "JMBG", "name": "Jedinstveni matični broj građana", "person_type": "individual", "format": "############", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "1234567890123"},
        {"type": "PIB", "name": "Poreski identifikacioni broj", "person_type": "company", "format": "########", "regex": r"^\d{9}$", "length": 9, "numeric_only": True, "example": "123456789"}
    ],
    "SGP": [
        {"type": "NRIC", "name": "National Registration Identity Card", "person_type": "individual", "format": "S#######A", "regex": r"^[STFG]\d{7}[A-Z]$", "length": 9, "numeric_only": False, "example": "S1234567A"},
        {"type": "UEN", "name": "Unique Entity Number", "person_type": "company", "format": "#########A", "regex": r"^\d{9,10}[A-Z]$", "length": 10, "numeric_only": False, "example": "123456789A"}
    ],
    "SVK": [
        {"type": "RC", "name": "Rodné číslo", "person_type": "individual", "format": "######/####", "regex": r"^\d{6}/?\d{4}$", "length": 10, "numeric_only": True, "example": "123456/7890"},
        {"type": "IČO", "name": "Identifikačné číslo organizácie", "person_type": "company", "format": "######", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"}
    ],
    "SVN": [
        {"type": "EMŠO", "name": "Enotna matična številka občana", "person_type": "individual", "format": "############", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "1234567890123"},
        {"type": "Matična", "name": "Matična številka", "person_type": "company", "format": "######", "regex": r"^\d{6}$", "length": 6, "numeric_only": True, "example": "123456"}
    ],
    "ZAF": [
        {"type": "ID", "name": "South African Identity Number", "person_type": "individual", "format": "## ## ## #### ###", "regex": r"^\d{13}$", "length": 13, "numeric_only": True, "example": "12 34 56 7890 123"},
        {"type": "CK", "name": "Company Key", "person_type": "company", "format": "####/######/##", "regex": r"^\d{4}/?\d{6}/?\d{2}$", "length": 14, "numeric_only": True, "example": "1234/567890/12"}
    ],
    "ESP": [
        {"type": "DNI", "name": "Documento Nacional de Identidad", "person_type": "individual", "format": "########-A", "regex": r"^\d{8}-?[A-Z]$", "length": 9, "numeric_only": False, "example": "12345678-A"},
        {"type": "NIE", "name": "Número de Identificación de Extranjero", "person_type": "individual", "format": "X#######-A", "regex": r"^[XYZ]\d{7}-?[A-Z]$", "length": 9, "numeric_only": False, "example": "X1234567-A"},
        {"type": "CIF", "name": "Código de Identificación Fiscal", "person_type": "company", "format": "A########", "regex": r"^[A-Z]\d{8}$", "length": 9, "numeric_only": False, "example": "A12345678"}
    ],
    "SWE": [
        {"type": "PNR", "name": "Personnummer", "person_type": "individual", "format": "######-####", "regex": r"^\d{6}-?\d{4}$", "length": 10, "numeric_only": True, "example": "123456-7890"},
        {"type": "Orgnr", "name": "Organisationsnummer", "person_type": "company", "format": "######-####", "regex": r"^\d{6}-?\d{4}$", "length": 10, "numeric_only": True, "example": "123456-7890"}
    ],
    "CHE": [
        {"type": "AHV", "name": "Alters- und Hinterlassenenversicherung", "person_type": "individual", "format": "###.##.###.###", "regex": r"^756\.?\d{4}\.?\d{4}\.?\d{2}$", "length": 13, "numeric_only": True, "example": "756.1234.5678.90"},
        {"type": "UID", "name": "Unternehmens-Identifikationsnummer", "person_type": "company", "format": "CHE-###.###.###", "regex": r"^CHE-?\d{3}\.?\d{3}\.?\d{3}$", "length": 12, "numeric_only": False, "example": "CHE-123.456.789"}
    ],
    # T
    "THA": [
        {"type": "ID", "name": "บัตรประจำตัวประชาชน", "person_type": "individual", "format": "#-####-#####-##-#", "regex": r"^\d-\d{4}-\d{5}-\d{2}-\d$", "length": 13, "numeric_only": True, "example": "1-2345-67890-12-3"},
        {"type": "TIN", "name": "เลขประจำตัวผู้เสียภาษี", "person_type": "both", "format": "###########", "regex": r"^\d{10,13}$", "length": 13, "numeric_only": True, "example": "1234567890123"}
    ],
    "TUR": [
        {"type": "TC Kimlik", "name": "Türkiye Cumhuriyeti Kimlik Numarası", "person_type": "individual", "format": "###########", "regex": r"^\d{11}$", "length": 11, "numeric_only": True, "example": "12345678901"},
        {"type": "Vergi No", "name": "Vergi Kimlik Numarası", "person_type": "company", "format": "#########", "regex": r"^\d{9,10}$", "length": 10, "numeric_only": True, "example": "1234567890"}
    ],
    "TWN": [
        {"type": "ID", "name": "身分證字號", "person_type": "individual", "format": "A#######A#", "regex": r"^[A-Z]\d{8}$", "length": 10, "numeric_only": False, "example": "A123456789"},
        {"type": "UBN", "name": "統一編號", "person_type": "company", "format": "########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"}
    ],
    # U
    "GBR": [
        {"type": "NINO", "name": "National Insurance Number", "person_type": "individual", "format": "AA ## ## ## A", "regex": r"^[A-Z]{2}\s?\d{2}\s?\d{2}\s?\d{2}\s?[A-D]$", "length": 9, "numeric_only": False, "example": "AB 12 34 56 C"},
        {"type": "UTR", "name": "Unique Taxpayer Reference", "person_type": "both", "format": "###### #####", "regex": r"^\d{10}$", "length": 10, "numeric_only": True, "example": "12345 67890"},
        {"type": "CRN", "name": "Company Registration Number", "person_type": "company", "format": "########", "regex": r"^\d{8}$", "length": 8, "numeric_only": True, "example": "12345678"}
    ],
    "USA": [
        {"type": "SSN", "name": "Social Security Number", "person_type": "individual", "format": "###-##-####", "regex": r"^\d{3}-?\d{2}-?\d{4}$", "length": 9, "numeric_only": True, "example": "123-45-6789"},
        {"type": "EIN", "name": "Employer Identification Number", "person_type": "company", "format": "##-#######", "regex": r"^\d{2}-?\d{7}$", "length": 9, "numeric_only": True, "example": "12-3456789"},
        {"type": "ITIN", "name": "Individual Taxpayer Identification Number", "person_type": "individual", "format": "9##-##-####", "regex": r"^9\d{2}-?\d{2}-?\d{4}$", "length": 9, "numeric_only": True, "example": "912-34-5678"}
    ],
    "UKR": [
        {"type": "КОАТУУ", "name": "Код згідно з КОАТУУ", "person_type": "both", "format": "##########", "regex": r"^\d{8,10}$", "length": 10, "numeric_only": True, "example": "12345678"}
    ],
    "URY": [
        {"type": "CI", "name": "Cédula de Identidad", "person_type": "individual", "format": "#.###.###-#", "regex": r"^\d\.?\d{3}\.?\d{3}-?\d$", "length": 8, "numeric_only": True, "example": "1.234.567-8"},
        {"type": "RUT", "name": "Registro Único de Contribuyente", "person_type": "both", "format": "#########", "regex": r"^\d{9}$", "length": 9, "numeric_only": True, "example": "123456789"}
    ],
    # V
    "VEN": [
        {"type": "CI", "name": "Cédula de Identidad", "person_type": "individual", "format": "V-##.###.###", "regex": r"^V-?\d{1,8}$", "length": 8, "numeric_only": True, "example": "V-12.345.678"},
        {"type": "RIF", "name": "Registro de Identificación Fiscal", "person_type": "company", "format": "A-##.###.###-#", "regex": r"^[VJEG]-?\d{8}-?\d$", "length": 10, "numeric_only": False, "example": "J-12345678-9"}
    ],
    "VNM": [
        {"type": "CMND", "name": "Chứng minh nhân dân", "person_type": "individual", "format": "############", "regex": r"^\d{9,12}$", "length": 12, "numeric_only": True, "example": "123456789012"},
        {"type": "MST", "name": "Mã số thuế", "person_type": "both", "format": "##########", "regex": r"^\d{10,14}$", "length": 14, "numeric_only": True, "example": "1234567890"}
    ],
}


def get_country_documents(iso3_code):
    """
    Retorna a lista de documentos para um país pelo código ISO3.
    Retorna lista vazia se país não encontrado.
    """
    return COUNTRY_DOCUMENTS.get(iso3_code, [])


def get_documents_by_type(iso3_code, person_type):
    """
    Retorna documentos filtrados por tipo de pessoa.

    person_type: 'individual', 'company', ou 'both'
    """
    documents = get_country_documents(iso3_code)
    return [doc for doc in documents if doc['person_type'] in [person_type, 'both']]


def get_individual_documents(iso3_code):
    """Retorna documentos para pessoa física."""
    return get_documents_by_type(iso3_code, 'individual')


def get_company_documents(iso3_code):
    """Retorna documentos para pessoa jurídica."""
    return get_documents_by_type(iso3_code, 'company')


def get_document_info(iso3_code, doc_type):
    """
    Retorna informações de um documento específico.

    Args:
        iso3_code: Código ISO3 do país
        doc_type: Sigla do documento (CPF, CNPJ, etc.)

    Returns:
        Dicionário com informações do documento ou None se não encontrado
    """
    documents = get_country_documents(iso3_code)
    for doc in documents:
        if doc['type'].upper() == doc_type.upper():
            return doc
    return None
