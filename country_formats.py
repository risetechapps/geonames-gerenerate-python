"""
Formatos de telefone, celular e código postal (zip code) por país.
Fontes: UPU (Universal Postal Union), ITU (International Telecommunication Union),
e regulamentos nacionais de telecomunicações.

Estrutura:
- country_code: código ISO3 do país
- zip_code_format: array de formatos aceitos (None se não aplicável)
- zip_code_regex: array de regex para validação
- telephone_format: array de formatos de telefone fixo
- telephone_regex: array de regex para telefone fixo
- cellphone_format: array de formatos de celular
- cellphone_regex: array de regex para celular
"""

COUNTRY_FORMATS = {
    # A
    "AFG": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+93 ## ### ####"],
        "telephone_regex": [r"^\+93\d{9}$"],
        "cellphone_format": ["+93 7# ### ####"],
        "cellphone_regex": [r"^\+93(70|71|72|73|74|75|76|77|78|79)\d{7}$"]
    },
    "ALB": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+355 ### ### ###"],
        "telephone_regex": [r"^\+355\d{9}$"],
        "cellphone_format": ["+355 6# ### ####"],
        "cellphone_regex": [r"^\+355(67|68|69)\d{7}$"]
    },
    "DZA": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+213 ### ## ## ##"],
        "telephone_regex": [r"^\+213\d{9}$"],
        "cellphone_format": ["+213 5## ## ## ##", "+213 6## ## ## ##", "+213 7## ## ## ##"],
        "cellphone_regex": [r"^\+213[567]\d{8}$"]
    },
    "AND": {
        "zip_code_format": ["AD###", "#####"],
        "zip_code_regex": [r"^AD\d{3}$", r"^\d{5}$"],
        "telephone_format": ["+376 ### ###"],
        "telephone_regex": [r"^\+376\d{6}$"],
        "cellphone_format": ["+376 3## ###", "+376 4## ###", "+376 6## ###"],
        "cellphone_regex": [r"^\+376[346]\d{5}$"]
    },
    "AGO": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+244 ### ### ###"],
        "telephone_regex": [r"^\+244\d{9}$"],
        "cellphone_format": ["+244 9## ### ###", "+244 92# ### ###"],
        "cellphone_regex": [r"^\+2449\d{8}$", r"^\+24492\d{7}$"]
    },
    "ARG": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+54 ## ####-####"],
        "telephone_regex": [r"^\+54\d{10}$"],
        "cellphone_format": ["+54 9 ## ####-####"],
        "cellphone_regex": [r"^\+549\d{10}$"]
    },
    "ARM": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+374 ## ######"],
        "telephone_regex": [r"^\+374\d{8}$"],
        "cellphone_format": ["+374 9# ######", "+374 4# ######", "+374 5# ######", "+374 7# ######", "+374 3# ######"],
        "cellphone_regex": [r"^\+374[93457]\d{7}$"]
    },
    "AUS": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+61 # #### ####"],
        "telephone_regex": [r"^\+61\d{9}$"],
        "cellphone_format": ["+61 4## ### ###"],
        "cellphone_regex": [r"^\+614\d{8}$"]
    },
    "AUT": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+43 ### #######"],
        "telephone_regex": [r"^\+43\d{9,11}$"],
        "cellphone_format": ["+43 6## #######", "+43 650 #######", "+43 676 #######", "+43 680 #######", "+43 681 #######", "+43 688 #######", "+43 699 #######"],
        "cellphone_regex": [r"^\+43[67]\d{8,9}$"]
    },
    "AZE": {
        "zip_code_format": ["AZ ####", "####"],
        "zip_code_regex": [r"^AZ\s?\d{4}$", r"^\d{4}$"],
        "telephone_format": ["+994 ## ### ## ##"],
        "telephone_regex": [r"^\+994\d{9}$"],
        "cellphone_format": ["+994 4# ### ## ##", "+994 5# ### ## ##", "+994 7# ### ## ##"],
        "cellphone_regex": [r"^\+994[457]\d{8}$"]
    },
    # B
    "BHS": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+1 242 ### ####"],
        "telephone_regex": [r"^\+1242\d{7}$"],
        "cellphone_format": ["+1 242 ### ####"],
        "cellphone_regex": [r"^\+1242\d{7}$"]
    },
    "BHR": {
        "zip_code_format": ["###", "####"],
        "zip_code_regex": [r"^\d{3,4}$"],
        "telephone_format": ["+973 #### ####"],
        "telephone_regex": [r"^\+973\d{8}$"],
        "cellphone_format": ["+973 3### ####"],
        "cellphone_regex": [r"^\+9733\d{7}$"]
    },
    "BGD": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+880 ####-######"],
        "telephone_regex": [r"^\+880\d{10}$"],
        "cellphone_format": ["+880 1###-######"],
        "cellphone_regex": [r"^\+8801[3-9]\d{8}$"]
    },
    "BRB": {
        "zip_code_format": ["BB#####", "#####"],
        "zip_code_regex": [r"^BB\d{5}$", r"^\d{5}$"],
        "telephone_format": ["+1 246 ### ####"],
        "telephone_regex": [r"^\+1246\d{7}$"],
        "cellphone_format": ["+1 246 ### ####"],
        "cellphone_regex": [r"^\+1246\d{7}$"]
    },
    "BLR": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+375 ## ###-##-##"],
        "telephone_regex": [r"^\+375\d{9}$"],
        "cellphone_format": ["+375 25 ###-##-##", "+375 29 ###-##-##", "+375 33 ###-##-##", "+375 44 ###-##-##"],
        "cellphone_regex": [r"^\+375(25|29|33|44)\d{7}$"]
    },
    "BEL": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+32 # ### ## ##"],
        "telephone_regex": [r"^\+32\d{8,9}$"],
        "cellphone_format": ["+32 4## ## ## ##"],
        "cellphone_regex": [r"^\+324\d{8}$"]
    },
    "BLZ": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+501 ### ####"],
        "telephone_regex": [r"^\+501\d{7}$"],
        "cellphone_format": ["+501 ### ####", "+501 6## ####"],
        "cellphone_regex": [r"^\+501\d{7}$"]
    },
    "BEN": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+229 ## ## ## ##"],
        "telephone_regex": [r"^\+229\d{8}$"],
        "cellphone_format": ["+229 ## ## ## ##"],
        "cellphone_regex": [r"^\+229\d{8}$"]
    },
    "BTN": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+975 # ### ###"],
        "telephone_regex": [r"^\+975\d{7,8}$"],
        "cellphone_format": ["+975 17 ## ## ##", "+975 77 ## ## ##"],
        "cellphone_regex": [r"^\+975[17]\d{7}$"]
    },
    "BOL": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+591 # #######"],
        "telephone_regex": [r"^\+591\d{8}$"],
        "cellphone_format": ["+591 6#######", "+591 7#######"],
        "cellphone_regex": [r"^\+591[67]\d{7}$"]
    },
    "BIH": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+387 ## ###-###"],
        "telephone_regex": [r"^\+387\d{8}$"],
        "cellphone_format": ["+387 6# ###-###", "+387 3# ###-###"],
        "cellphone_regex": [r"^\+387[63]\d{7}$"]
    },
    "BWA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+267 ## ### ###"],
        "telephone_regex": [r"^\+267\d{7,8}$"],
        "cellphone_format": ["+267 71 ### ###", "+267 72 ### ###", "+267 73 ### ###", "+267 74 ### ###", "+267 75 ### ###", "+267 76 ### ###"],
        "cellphone_regex": [r"^\+2677[1-6]\d{6}$"]
    },
    "BRA": {
        "zip_code_format": ["#####-###"],
        "zip_code_regex": [r"^\d{5}-\d{3}$", r"^\d{8}$"],
        "telephone_format": ["+55 (##) ####-####", "+55 (##) #####-####"],
        "telephone_regex": [r"^\+55\d{10,11}$"],
        "cellphone_format": ["+55 (##) #####-####"],
        "cellphone_regex": [r"^\+55\d{2}9\d{8}$"]
    },
    "BRN": {
        "zip_code_format": ["BB####", "####"],
        "zip_code_regex": [r"^BB\d{4}$", r"^\d{4}$"],
        "telephone_format": ["+673 ### ####"],
        "telephone_regex": [r"^\+673\d{7}$"],
        "cellphone_format": ["+673 ### ####", "+673 7## ####", "+673 8## ####"],
        "cellphone_regex": [r"^\+673\d{7}$"]
    },
    "BGR": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+359 ### ### ###"],
        "telephone_regex": [r"^\+359\d{9}$"],
        "cellphone_format": ["+359 87# ### ###", "+359 88# ### ###", "+359 89# ### ###"],
        "cellphone_regex": [r"^\+3598[789]\d{7}$"]
    },
    "BFA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+226 ## ## ## ##"],
        "telephone_regex": [r"^\+226\d{8}$"],
        "cellphone_format": ["+226 ## ## ## ##"],
        "cellphone_regex": [r"^\+226\d{8}$"]
    },
    "BDI": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+257 ## ## ## ##"],
        "telephone_regex": [r"^\+257\d{8}$"],
        "cellphone_format": ["+257 ## ## ## ##", "+257 69 ## ## ##", "+257 71 ## ## ##", "+257 75 ## ## ##", "+257 76 ## ## ##", "+257 77 ## ## ##", "+257 79 ## ## ##"],
        "cellphone_regex": [r"^\+257\d{8}$"]
    },
    "KHM": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+855 ## ### ###"],
        "telephone_regex": [r"^\+855\d{8,9}$"],
        "cellphone_format": ["+855 1## ### ###", "+855 6## ### ###", "+855 7## ### ###", "+855 8## ### ###", "+855 9## ### ###"],
        "cellphone_regex": [r"^\+855[16789]\d{7,8}$"]
    },
    "CMR": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+237 ## ## ## ##"],
        "telephone_regex": [r"^\+237\d{8}$"],
        "cellphone_format": ["+237 6## ## ## ##"],
        "cellphone_regex": [r"^\+2376\d{8}$"]
    },
    "CAN": {
        "zip_code_format": ["A#A #A#"],
        "zip_code_regex": [r"^[A-Za-z]\d[A-Za-z][\s-]?\d[A-Za-z]\d$"],
        "telephone_format": ["+1 ### ###-####"],
        "telephone_regex": [r"^\+1\d{10}$"],
        "cellphone_format": ["+1 ### ###-####"],
        "cellphone_regex": [r"^\+1\d{10}$"]
    },
    "CPV": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+238 ### ####"],
        "telephone_regex": [r"^\+238\d{7}$"],
        "cellphone_format": ["+238 ### ####", "+238 5## ####", "+238 9## ####"],
        "cellphone_regex": [r"^\+238[59]\d{6}$"]
    },
    "CAF": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+236 ## ## ## ##"],
        "telephone_regex": [r"^\+236\d{8}$"],
        "cellphone_format": ["+236 70 ## ## ##", "+236 72 ## ## ##", "+236 75 ## ## ##", "+236 77 ## ## ##"],
        "cellphone_regex": [r"^\+2367[0275]\d{7}$"]
    },
    "TCD": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+235 ## ## ## ##"],
        "telephone_regex": [r"^\+235\d{8}$"],
        "cellphone_format": ["+235 6## ## ## ##", "+235 7## ## ## ##", "+235 9## ## ## ##"],
        "cellphone_regex": [r"^\+235[679]\d{7}$"]
    },
    "CHL": {
        "zip_code_format": ["#######", "###-####"],
        "zip_code_regex": [r"^\d{7}$", r"^\d{3}-\d{4}$"],
        "telephone_format": ["+56 # #### ####"],
        "telephone_regex": [r"^\+56\d{9}$"],
        "cellphone_format": ["+56 9 #### ####"],
        "cellphone_regex": [r"^\+569\d{8}$"]
    },
    "CHN": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+86 ### #### ####", "+86 #### #### ####"],
        "telephone_regex": [r"^\+86\d{10,12}$"],
        "cellphone_format": ["+86 1## #### ####"],
        "cellphone_regex": [r"^\+861[3-9]\d{9}$"]
    },
    "COL": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+57 ### #######", "+57 ## #######"],
        "telephone_regex": [r"^\+57\d{9,10}$"],
        "cellphone_format": ["+57 ### #######"],
        "cellphone_regex": [r"^\+57\d{9,10}$"]
    },
    "COM": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+269 ### ## ##"],
        "telephone_regex": [r"^\+269\d{7}$"],
        "cellphone_format": ["+269 ### ## ##", "+269 3## ## ##", "+269 4## ## ##"],
        "cellphone_regex": [r"^\+269[34]?\d{6}$"]
    },
    "COG": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+242 ## ### ####"],
        "telephone_regex": [r"^\+242\d{9}$"],
        "cellphone_format": ["+242 0# ### ####", "+242 4# ### ####", "+242 5# ### ####", "+242 6# ### ####"],
        "cellphone_regex": [r"^\+242[0456]\d{8}$"]
    },
    "COD": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+243 ### ### ###"],
        "telephone_regex": [r"^\+243\d{9}$"],
        "cellphone_format": ["+243 8# ### ###", "+243 9# ### ###"],
        "cellphone_regex": [r"^\+243[89]\d{8}$"]
    },
    "CRI": {
        "zip_code_format": ["#####", "####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{4}-\d{4}$"],
        "telephone_format": ["+506 #### ####"],
        "telephone_regex": [r"^\+506\d{8}$"],
        "cellphone_format": ["+506 #### ####", "+506 6### ####", "+506 7### ####", "+506 8### ####"],
        "cellphone_regex": [r"^\+506[678]?\d{7}$"]
    },
    "HRV": {
        "zip_code_format": ["#####", "HR-#####"],
        "zip_code_regex": [r"^\d{5}$", r"^HR-\d{5}$"],
        "telephone_format": ["+385 # ### ####"],
        "telephone_regex": [r"^\+385\d{8,9}$"],
        "cellphone_format": ["+385 9# ### ####", "+385 9## ### ####"],
        "cellphone_regex": [r"^\+3859\d{7,8}$"]
    },
    "CUB": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+53 # #######"],
        "telephone_regex": [r"^\+53\d{8}$"],
        "cellphone_format": ["+53 5 #######"],
        "cellphone_regex": [r"^\+535\d{7}$"]
    },
    "CYP": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+357 ## ######"],
        "telephone_regex": [r"^\+357\d{8}$"],
        "cellphone_format": ["+357 9# ######", "+357 9## ######", "+357 97 ######"],
        "cellphone_regex": [r"^\+3579\d{7}$"]
    },
    "CZE": {
        "zip_code_format": ["### ##", "#####"],
        "zip_code_regex": [r"^\d{3}\s?\d{2}$", r"^\d{5}$"],
        "telephone_format": ["+420 ### ### ###"],
        "telephone_regex": [r"^\+420\d{9}$"],
        "cellphone_format": ["+420 ### ### ###", "+420 6## ### ###", "+420 7## ### ###"],
        "cellphone_regex": [r"^\+420[67]?\d{8}$"]
    },
    # D
    "DNK": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+45 ## ## ## ##"],
        "telephone_regex": [r"^\+45\d{8}$"],
        "cellphone_format": ["+45 ## ## ## ##", "+45 2# ## ## ##", "+45 3# ## ## ##", "+45 4# ## ## ##", "+45 5# ## ## ##", "+45 6# ## ## ##"],
        "cellphone_regex": [r"^\+45\d{8}$"]
    },
    "DJI": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+253 ## ## ## ##"],
        "telephone_regex": [r"^\+253\d{8}$"],
        "cellphone_format": ["+253 77 ## ## ##", "+253 78 ## ## ##"],
        "cellphone_regex": [r"^\+2537[78]\d{6}$"]
    },
    "DMA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+1 767 ### ####"],
        "telephone_regex": [r"^\+1767\d{7}$"],
        "cellphone_format": ["+1 767 ### ####", "+1 767 2## ####", "+1 767 3## ####", "+1 767 5## ####", "+1 767 7## ####"],
        "cellphone_regex": [r"^\+1767\d{7}$"]
    },
    "DOM": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+1 809 ### ####", "+1 829 ### ####", "+1 849 ### ####"],
        "telephone_regex": [r"^\+1(809|829|849)\d{7}$"],
        "cellphone_format": ["+1 809 ### ####", "+1 829 ### ####", "+1 849 ### ####"],
        "cellphone_regex": [r"^\+1(809|829|849)\d{7}$"]
    },
    # E
    "ECU": {
        "zip_code_format": ["######", "EC######"],
        "zip_code_regex": [r"^\d{6}$", r"^EC\d{6}$"],
        "telephone_format": ["+593 # ### ####", "+593 ## ### ####"],
        "telephone_regex": [r"^\+593\d{8,9}$"],
        "cellphone_format": ["+593 9# ### ####", "+593 9## ### ####"],
        "cellphone_regex": [r"^\+5939\d{8}$"]
    },
    "EGY": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+20 ### #######"],
        "telephone_regex": [r"^\+20\d{10}$"],
        "cellphone_format": ["+20 1# ### ####", "+20 1## ### ####"],
        "cellphone_regex": [r"^\+201\d{9,10}$"]
    },
    "SLV": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+503 #### ####"],
        "telephone_regex": [r"^\+503\d{8}$"],
        "cellphone_format": ["+503 #### ####", "+503 6### ####", "+503 7### ####"],
        "cellphone_regex": [r"^\+503[67]?\d{7}$"]
    },
    "GNQ": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+240 ## ### ####"],
        "telephone_regex": [r"^\+240\d{9}$"],
        "cellphone_format": ["+240 2## ### ###", "+240 5## ### ###", "+240 6## ### ###", "+240 7## ### ###"],
        "cellphone_regex": [r"^\+240[2567]\d{8}$"]
    },
    "ERI": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+291 # ### ###"],
        "telephone_regex": [r"^\+291\d{7}$"],
        "cellphone_format": ["+291 7# ### ###", "+291 1# ### ###"],
        "cellphone_regex": [r"^\+291[17]\d{6}$"]
    },
    "EST": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+372 #### ####"],
        "telephone_regex": [r"^\+372\d{7,8}$"],
        "cellphone_format": ["+372 5### ####"],
        "cellphone_regex": [r"^\+3725\d{7}$"]
    },
    "ETH": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+251 ## ### ####"],
        "telephone_regex": [r"^\+251\d{9}$"],
        "cellphone_format": ["+251 9# ### ####"],
        "cellphone_regex": [r"^\+2519\d{8}$"]
    },
    # F
    "FJI": {
        "zip_code_format": ["#####", "FJ#####"],
        "zip_code_regex": [r"^\d{5}$", r"^FJ\d{5}$"],
        "telephone_format": ["+679 ### ####"],
        "telephone_regex": [r"^\+679\d{7}$"],
        "cellphone_format": ["+679 ### ####", "+679 7## ####", "+679 8## ####", "+679 9## ####"],
        "cellphone_regex": [r"^\+679[789]?\d{6}$"]
    },
    "FIN": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+358 ## ### ## ##"],
        "telephone_regex": [r"^\+358\d{8,9}$"],
        "cellphone_format": ["+358 4# ### ## ##", "+358 5# ### ## ##"],
        "cellphone_regex": [r"^\+358[45]\d{8}$"]
    },
    "FRA": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+33 # ## ## ## ##"],
        "telephone_regex": [r"^\+33\d{9}$"],
        "cellphone_format": ["+33 6 ## ## ## ##", "+33 7 ## ## ## ##"],
        "cellphone_regex": [r"^\+33[67]\d{8}$"]
    },
    # G
    "GAB": {
        "zip_code_format": ["#####", "## ###"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{2}\s\d{3}$"],
        "telephone_format": ["+241 ## ## ## ##"],
        "telephone_regex": [r"^\+241\d{7,8}$"],
        "cellphone_format": ["+241 0# ## ## ##", "+241 2## ## ## ##", "+241 3## ## ## ##", "+241 4## ## ## ##", "+241 5## ## ## ##", "+241 6## ## ## ##", "+241 7## ## ## ##"],
        "cellphone_regex": [r"^\+241[0234567]\d{6,7}$"]
    },
    "GMB": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+220 ### ####"],
        "telephone_regex": [r"^\+220\d{7}$"],
        "cellphone_format": ["+220 ### ####", "+220 7## ####", "+220 9## ####"],
        "cellphone_regex": [r"^\+220[79]?\d{6}$"]
    },
    "GEO": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+995 ### ## ## ##"],
        "telephone_regex": [r"^\+995\d{9}$"],
        "cellphone_format": ["+995 5## ## ## ##", "+995 51# ## ## ##", "+995 55# ## ## ##", "+995 57# ## ## ##", "+995 58# ## ## ##", "+995 59# ## ## ##"],
        "cellphone_regex": [r"^\+9955\d{8}$"]
    },
    "DEU": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+49 ### #######", "+49 #### #######", "+49 ### ########"],
        "telephone_regex": [r"^\+49\d{10,11}$"],
        "cellphone_format": ["+49 15## #######", "+49 16## #######", "+49 17## #######"],
        "cellphone_regex": [r"^\+491[567]\d{8,9}$"]
    },
    "GHA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+233 ## ### ####"],
        "telephone_regex": [r"^\+233\d{9}$"],
        "cellphone_format": ["+233 2# ### ####", "+233 5# ### ####", "+233 2## ### ####", "+233 5## ### ####"],
        "cellphone_regex": [r"^\+233[25]\d{8}$"]
    },
    "GRC": {
        "zip_code_format": ["### ##", "#####"],
        "zip_code_regex": [r"^\d{3}\s?\d{2}$", r"^\d{5}$"],
        "telephone_format": ["+30 ### ### ####"],
        "telephone_regex": [r"^\+30\d{10}$"],
        "cellphone_format": ["+30 6## ### ####"],
        "cellphone_regex": [r"^\+306\d{9}$"]
    },
    "GRD": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+1 473 ### ####"],
        "telephone_regex": [r"^\+1473\d{7}$"],
        "cellphone_format": ["+1 473 ### ####", "+1 473 4## ####", "+1 473 5## ####", "+1 473 7## ####"],
        "cellphone_regex": [r"^\+1473\d{7}$"]
    },
    "GTM": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+502 #### ####"],
        "telephone_regex": [r"^\+502\d{8}$"],
        "cellphone_format": ["+502 #### ####", "+502 3### ####", "+502 4### ####", "+502 5### ####"],
        "cellphone_regex": [r"^\+502[345]?\d{7}$"]
    },
    "GIN": {
        "zip_code_format": ["###", "####"],
        "zip_code_regex": [r"^\d{3,4}$"],
        "telephone_format": ["+224 ### ## ## ##"],
        "telephone_regex": [r"^\+224\d{9}$"],
        "cellphone_format": ["+224 6## ## ## ##", "+224 622 ## ## ##", "+224 623 ## ## ##", "+224 624 ## ## ##", "+224 625 ## ## ##", "+224 626 ## ## ##", "+224 627 ## ## ##", "+224 628 ## ## ##", "+224 629 ## ## ##", "+224 630 ## ## ##", "+224 631 ## ## ##"],
        "cellphone_regex": [r"^\+224[6]\d{8}$"]
    },
    "GNB": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+245 ### ####"],
        "telephone_regex": [r"^\+245\d{7}$"],
        "cellphone_format": ["+245 ### ####", "+245 5## ####", "+245 6## ####", "+245 7## ####"],
        "cellphone_regex": [r"^\+245[567]?\d{6}$"]
    },
    "GUY": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+592 ### ####"],
        "telephone_regex": [r"^\+592\d{7}$"],
        "cellphone_format": ["+592 ### ####", "+592 5## ####", "+592 6## ####"],
        "cellphone_regex": [r"^\+592[56]?\d{6}$"]
    },
    # H
    "HTI": {
        "zip_code_format": ["HT####", "####"],
        "zip_code_regex": [r"^HT\d{4}$", r"^\d{4}$"],
        "telephone_format": ["+509 ## ## ####"],
        "telephone_regex": [r"^\+509\d{8}$"],
        "cellphone_format": ["+509 ## ## ####", "+509 3# ## ####", "+509 4# ## ####"],
        "cellphone_regex": [r"^\+509[34]?\d{7}$"]
    },
    "HND": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+504 ####-####"],
        "telephone_regex": [r"^\+504\d{8}$"],
        "cellphone_format": ["+504 ####-####", "+504 3###-####", "+504 8###-####", "+504 9###-####"],
        "cellphone_regex": [r"^\+504[389]?\d{7}$"]
    },
    "HUN": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+36 # ### ####"],
        "telephone_regex": [r"^\+36\d{8,9}$"],
        "cellphone_format": ["+36 2# ### ####", "+36 3# ### ####", "+36 7# ### ####"],
        "cellphone_regex": [r"^\+36[237]\d{8}$"]
    },
    # I
    "ISL": {
        "zip_code_format": ["###"],
        "zip_code_regex": [r"^\d{3}$"],
        "telephone_format": ["+354 ### ####"],
        "telephone_regex": [r"^\+354\d{7}$"],
        "cellphone_format": ["+354 ### ####", "+354 6## ####", "+354 7## ####", "+354 8## ####"],
        "cellphone_regex": [r"^\+354[678]?\d{6}$"]
    },
    "IND": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+91 ##### #####"],
        "telephone_regex": [r"^\+91\d{10}$"],
        "cellphone_format": ["+91 6#########", "+91 7#########", "+91 8#########", "+91 9#########"],
        "cellphone_regex": [r"^\+91[6-9]\d{9}$"]
    },
    "IDN": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+62 ### ### ####", "+62 ## ### ####", "+62 ### #### ####"],
        "telephone_regex": [r"^\+62\d{9,11}$"],
        "cellphone_format": ["+62 8## ### ####", "+62 8### ### ####"],
        "cellphone_regex": [r"^\+628\d{9,10}$"]
    },
    "IRN": {
        "zip_code_format": ["##########", "#####-#####"],
        "zip_code_regex": [r"^\d{10}$", r"^\d{5}-\d{5}$"],
        "telephone_format": ["+98 ### #### ####"],
        "telephone_regex": [r"^\+98\d{10}$"],
        "cellphone_format": ["+98 9## ### ####", "+98 9### ### ####"],
        "cellphone_regex": [r"^\+989\d{9,10}$"]
    },
    "IRQ": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+964 ### ### ####"],
        "telephone_regex": [r"^\+964\d{9,10}$"],
        "cellphone_format": ["+964 7## ### ####"],
        "cellphone_regex": [r"^\+9647\d{9}$"]
    },
    "IRL": {
        "zip_code_format": ["A## A###", "A## AA###"],
        "zip_code_regex": [r"^[A-Z]\d{2}\s?[A-Z]?\d{3}$"],
        "telephone_format": ["+353 ## ### ####"],
        "telephone_regex": [r"^\+353\d{9}$"],
        "cellphone_format": ["+353 8# ### ####", "+353 83 ### ####", "+353 85 ### ####", "+353 86 ### ####", "+353 87 ### ####", "+353 88 ### ####", "+353 89 ### ####"],
        "cellphone_regex": [r"^\+3538\d{8}$"]
    },
    "ISR": {
        "zip_code_format": ["#######"],
        "zip_code_regex": [r"^\d{7}$"],
        "telephone_format": ["+972 ## ### ####"],
        "telephone_regex": [r"^\+972\d{8,9}$"],
        "cellphone_format": ["+972 5# ### ####"],
        "cellphone_regex": [r"^\+9725\d{8}$"]
    },
    "ITA": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+39 ### #### ###"],
        "telephone_regex": [r"^\+39\d{9,10}$"],
        "cellphone_format": ["+39 3## ### ###"],
        "cellphone_regex": [r"^\+393\d{8,9}$"]
    },
    "CIV": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+225 ## ## ## ##"],
        "telephone_regex": [r"^\+225\d{8}$"],
        "cellphone_format": ["+225 0# ## ## ##", "+225 4# ## ## ##", "+225 5# ## ## ##", "+225 6# ## ## ##", "+225 7# ## ## ##", "+225 8# ## ## ##", "+225 9# ## ## ##"],
        "cellphone_regex": [r"^\+225[0456789]\d{7}$"]
    },
    # J
    "JAM": {
        "zip_code_format": ["##", "#####"],
        "zip_code_regex": [r"^\d{2}$", r"^\d{5}$"],
        "telephone_format": ["+1 876 ### ####"],
        "telephone_regex": [r"^\+1876\d{7}$"],
        "cellphone_format": ["+1 876 ### ####", "+1 876 3## ####", "+1 876 4## ####", "+1 876 5## ####", "+1 876 6## ####", "+1 876 7## ####", "+1 876 8## ####"],
        "cellphone_regex": [r"^\+1876\d{7}$"]
    },
    "JPN": {
        "zip_code_format": ["###-####"],
        "zip_code_regex": [r"^\d{3}-?\d{4}$"],
        "telephone_format": ["+81 ##-####-####", "+81 ###-####-####", "+81 ####-####-####"],
        "telephone_regex": [r"^\+81\d{9,10}$"],
        "cellphone_format": ["+81 70-####-####", "+81 80-####-####", "+81 90-####-####"],
        "cellphone_regex": [r"^\+81[789]0\d{8}$"]
    },
    "JOR": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+962 # #### ####"],
        "telephone_regex": [r"^\+962\d{8,9}$"],
        "cellphone_format": ["+962 7# #### ####"],
        "cellphone_regex": [r"^\+9627\d{8}$"]
    },
    # K
    "KAZ": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+7 ### ### ## ##", "+7 # ### ### ## ##"],
        "telephone_regex": [r"^\+7\d{10,11}$"],
        "cellphone_format": ["+7 70# ### ## ##", "+7 74# ### ## ##", "+7 75# ### ## ##", "+7 76# ### ## ##", "+7 77# ### ## ##"],
        "cellphone_regex": [r"^\+77[04567]\d{8}$"]
    },
    "KEN": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+254 ### ######"],
        "telephone_regex": [r"^\+254\d{9}$"],
        "cellphone_format": ["+254 7## ######", "+254 1## ######"],
        "cellphone_regex": [r"^\+254[17]\d{8}$"]
    },
    "KIR": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+686 ## ###"],
        "telephone_regex": [r"^\+686\d{5}$"],
        "cellphone_format": ["+686 ## ###", "+686 7## ###", "+686 8## ###", "+686 9## ###"],
        "cellphone_regex": [r"^\+686[789]?\d{4}$"]
    },
    "KWT": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+965 #### ####"],
        "telephone_regex": [r"^\+965\d{8}$"],
        "cellphone_format": ["+965 5## ####", "+965 6## ####", "+965 9## ####"],
        "cellphone_regex": [r"^\+965[569]\d{6}$"]
    },
    "KGZ": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+996 ### ### ###"],
        "telephone_regex": [r"^\+996\d{9}$"],
        "cellphone_format": ["+996 5## ### ###", "+996 7## ### ###"],
        "cellphone_regex": [r"^\+996[57]\d{8}$"]
    },
    # L
    "LAO": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+856 ## ## ### ###", "+856 ## ## ## ###"],
        "telephone_regex": [r"^\+856\d{8,9}$"],
        "cellphone_format": ["+856 20 ## ### ###", "+856 30 ## ### ###"],
        "cellphone_regex": [r"^\+856[23]0\d{7}$"]
    },
    "LVA": {
        "zip_code_format": ["LV-####", "####"],
        "zip_code_regex": [r"^LV-\d{4}$", r"^\d{4}$"],
        "telephone_format": ["+371 ## ### ###"],
        "telephone_regex": [r"^\+371\d{8}$"],
        "cellphone_format": ["+371 2## ## ###"],
        "cellphone_regex": [r"^\+3712\d{7}$"]
    },
    "LBN": {
        "zip_code_format": ["#### ####", "#####"],
        "zip_code_regex": [r"^\d{4}\s\d{4}$", r"^\d{4,5}$"],
        "telephone_format": ["+961 # ### ###"],
        "telephone_regex": [r"^\+961\d{7,8}$"],
        "cellphone_format": ["+961 3# ### ###", "+961 7# ### ###", "+961 8# ### ###"],
        "cellphone_regex": [r"^\+961[378]\d{7}$"]
    },
    "LSO": {
        "zip_code_format": ["###"],
        "zip_code_regex": [r"^\d{3}$"],
        "telephone_format": ["+266 #### ####"],
        "telephone_regex": [r"^\+266\d{8}$"],
        "cellphone_format": ["+266 5### ####", "+266 6### ####", "+266 8### ####"],
        "cellphone_regex": [r"^\+266[568]\d{6}$"]
    },
    "LBR": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+231 ## ### ####"],
        "telephone_regex": [r"^\+231\d{7,9}$"],
        "cellphone_format": ["+231 4## ### ####", "+231 5## ### ####", "+231 6## ### ####", "+231 7## ### ####", "+231 8## ### ####"],
        "cellphone_regex": [r"^\+231[45678]\d{6}$"]
    },
    "LBY": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+218 ## ### ####", "+218 ### ### ###"],
        "telephone_regex": [r"^\+218\d{8,9}$"],
        "cellphone_format": ["+218 9# ### ####", "+218 91# ### ###", "+218 92# ### ###", "+218 94# ### ###"],
        "cellphone_regex": [r"^\+2189[124]?\d{7}$"]
    },
    "LIE": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+423 ### ## ##"],
        "telephone_regex": [r"^\+423\d{7}$"],
        "cellphone_format": ["+423 7## ## ##", "+423 6## ## ##"],
        "cellphone_regex": [r"^\+423[67]\d{6}$"]
    },
    "LTU": {
        "zip_code_format": ["LT-#####", "#####"],
        "zip_code_regex": [r"^LT-\d{5}$", r"^\d{5}$"],
        "telephone_format": ["+370 ### #####"],
        "telephone_regex": [r"^\+370\d{8}$"],
        "cellphone_format": ["+370 6## #####"],
        "cellphone_regex": [r"^\+3706\d{7}$"]
    },
    "LUX": {
        "zip_code_format": ["L-####", "####"],
        "zip_code_regex": [r"^L-\d{4}$", r"^\d{4}$"],
        "telephone_format": ["+352 ### ### ###"],
        "telephone_regex": [r"^\+352\d{6,9}$"],
        "cellphone_format": ["+352 6## ### ###", "+352 621 ### ###", "+352 65# ### ###"],
        "cellphone_regex": [r"^\+352[56]\d{7}$"]
    },
    # M
    "MDG": {
        "zip_code_format": ["###"],
        "zip_code_regex": [r"^\d{3}$"],
        "telephone_format": ["+261 ## ## ### ##"],
        "telephone_regex": [r"^\+261\d{9}$"],
        "cellphone_format": ["+261 32 ## ### ##", "+261 33 ## ### ##", "+261 34 ## ### ##"],
        "cellphone_regex": [r"^\+2613[234]\d{7}$"]
    },
    "MWI": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+265 ## ### ####"],
        "telephone_regex": [r"^\+265\d{9}$"],
        "cellphone_format": ["+265 88# ### ###", "+265 99# ### ###", "+265 88 #### ####", "+265 99 #### ####"],
        "cellphone_regex": [r"^\+265[89]{2}\d{7}$"]
    },
    "MYS": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+60 ##-### ####", "+60 ###-### ####", "+60 #-### ####"],
        "telephone_regex": [r"^\+60\d{9,10}$"],
        "cellphone_format": ["+60 1#-### ####", "+60 1##-### ####"],
        "cellphone_regex": [r"^\+601\d{8,9}$"]
    },
    "MDV": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+960 ### ####"],
        "telephone_regex": [r"^\+960\d{7}$"],
        "cellphone_format": ["+960 7## ####", "+960 9## ####", "+960 95# ####"],
        "cellphone_regex": [r"^\+960[79]\d{6}$"]
    },
    "MLI": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+223 ## ## ## ##"],
        "telephone_regex": [r"^\+223\d{8}$"],
        "cellphone_format": ["+223 7# ## ## ##", "+223 8# ## ## ##", "+223 9# ## ## ##"],
        "cellphone_regex": [r"^\+223[789]\d{7}$"]
    },
    "MLT": {
        "zip_code_format": ["AAA ####", "AAA ## ##", "AAA ###", "AAA ####"],
        "zip_code_regex": [r"^[A-Z]{3}\s?\d{4}$", r"^[A-Z]{3}\s?\d{2}\s?\d{2}$"],
        "telephone_format": ["+356 #### ####"],
        "telephone_regex": [r"^\+356\d{8}$"],
        "cellphone_format": ["+356 79## ####", "+356 77## ####", "+356 98## ####", "+356 99## ####"],
        "cellphone_regex": [r"^\+356[79]\d{6}$"]
    },
    "MHL": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+692 ### ####"],
        "telephone_regex": [r"^\+692\d{7}$"],
        "cellphone_format": ["+692 ### ####", "+692 4## ####", "+692 5## ####"],
        "cellphone_regex": [r"^\+692[45]?\d{6}$"]
    },
    "MRT": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+222 ## ## ## ##"],
        "telephone_regex": [r"^\+222\d{8}$"],
        "cellphone_format": ["+222 2# ## ## ##", "+222 3# ## ## ##", "+222 4# ## ## ##"],
        "cellphone_regex": [r"^\+222[234]\d{7}$"]
    },
    "MUS": {
        "zip_code_format": ["#####", "AA####"],
        "zip_code_regex": [r"^\d{5}$", r"^[A-Z]{2}\d{4}$"],
        "telephone_format": ["+230 #### ####"],
        "telephone_regex": [r"^\+230\d{7}$"],
        "cellphone_format": ["+230 5### ####", "+230 5## ####"],
        "cellphone_regex": [r"^\+2305\d{6}$"]
    },
    "MEX": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+52 ### ### ####", "+52 ## #### ####"],
        "telephone_regex": [r"^\+52\d{10}$"],
        "cellphone_format": ["+52 1 ### ### ####", "+52 ## #### ####", "+52 ### ### ####"],
        "cellphone_regex": [r"^\+52\d{10,11}$"]
    },
    "FSM": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+691 ### ####"],
        "telephone_regex": [r"^\+691\d{7}$"],
        "cellphone_format": ["+691 ### ####"],
        "cellphone_regex": [r"^\+691\d{7}$"]
    },
    "MDA": {
        "zip_code_format": ["MD-####", "####"],
        "zip_code_regex": [r"^MD-\d{4}$", r"^\d{4}$"],
        "telephone_format": ["+373 ### ## ###"],
        "telephone_regex": [r"^\+373\d{8}$"],
        "cellphone_format": ["+373 6## ## ###", "+373 7## ## ###"],
        "cellphone_regex": [r"^\+373[67]\d{7}$"]
    },
    "MCO": {
        "zip_code_format": ["980##"],
        "zip_code_regex": [r"^980\d{2}$"],
        "telephone_format": ["+377 ## ## ## ##"],
        "telephone_regex": [r"^\+377\d{8}$"],
        "cellphone_format": ["+377 ## ## ## ##", "+377 4## ## ## ##", "+377 6## ## ## ##"],
        "cellphone_regex": [r"^\+377[46]?\d{8}$"]
    },
    "MNG": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+976 ## ## ####"],
        "telephone_regex": [r"^\+976\d{8}$"],
        "cellphone_format": ["+976 8## ####", "+976 9## ####"],
        "cellphone_regex": [r"^\+976[89]\d{6}$"]
    },
    "MNE": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+382 ## ### ###"],
        "telephone_regex": [r"^\+382\d{8}$"],
        "cellphone_format": ["+382 6# ### ###", "+382 67 ### ###", "+382 68 ### ###", "+382 69 ### ###"],
        "cellphone_regex": [r"^\+382[67]\d{7}$"]
    },
    "MAR": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+212 ## #### ###"],
        "telephone_regex": [r"^\+212\d{9}$"],
        "cellphone_format": ["+212 6##-## ## ##", "+212 7##-## ## ##"],
        "cellphone_regex": [r"^\+212[67]\d{8}$"]
    },
    "MOZ": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+258 ## ### ####"],
        "telephone_regex": [r"^\+258\d{9}$"],
        "cellphone_format": ["+258 8## ### ###", "+258 82# ### ###", "+258 83# ### ###", "+258 84# ### ###", "+258 85# ### ###", "+258 86# ### ###", "+258 87# ### ###"],
        "cellphone_regex": [r"^\+2588[234567]?\d{7}$"]
    },
    "MMR": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+95 ## ### ###", "+95 ### ### ###", "+95 #### ### ###"],
        "telephone_regex": [r"^\+95\d{7,10}$"],
        "cellphone_format": ["+95 9## ### ###", "+95 9### ### ###", "+95 9#### ### ###"],
        "cellphone_regex": [r"^\+959\d{8,10}$"]
    },
    # N
    "NAM": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+264 ## ### ####"],
        "telephone_regex": [r"^\+264\d{9}$"],
        "cellphone_format": ["+264 81## ### ###", "+264 82## ### ###", "+264 84## ### ###", "+264 85## ### ###"],
        "cellphone_regex": [r"^\+2648[1245]\d{7}$"]
    },
    "NRU": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+674 ### ####"],
        "telephone_regex": [r"^\+674\d{7}$"],
        "cellphone_format": ["+674 ### ####", "+674 5## ####", "+674 7## ####"],
        "cellphone_regex": [r"^\+674[57]?\d{6}$"]
    },
    "NPL": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+977 ## #### ####"],
        "telephone_regex": [r"^\+977\d{9}$"],
        "cellphone_format": ["+977 98########", "+977 97########", "+977 96########"],
        "cellphone_regex": [r"^\+9779[678]\d{8}$"]
    },
    "NLD": {
        "zip_code_format": ["#### ##", "####"],
        "zip_code_regex": [r"^\d{4}\s[A-Z]{2}$", r"^\d{4}$"],
        "telephone_format": ["+31 ## ### ####", "+31 ### ## ## ##"],
        "telephone_regex": [r"^\+31\d{9}$"],
        "cellphone_format": ["+31 6 ## ## ## ##", "+31 6# ## ## ## ##"],
        "cellphone_regex": [r"^\+316\d{8}$"]
    },
    "NZL": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+64 # ### ####", "+64 ## ### ####", "+64 ### ### ####"],
        "telephone_regex": [r"^\+64\d{8,10}$"],
        "cellphone_format": ["+64 2# ### ###", "+64 2## ### ###", "+64 2### ### ###"],
        "cellphone_regex": [r"^\+642\d{7,9}$"]
    },
    "NIC": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+505 #### ####"],
        "telephone_regex": [r"^\+505\d{8}$"],
        "cellphone_format": ["+505 #### ####", "+505 8### ####"],
        "cellphone_regex": [r"^\+505[8]?\d{7}$"]
    },
    "NER": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+227 ## ## ## ##"],
        "telephone_regex": [r"^\+227\d{8}$"],
        "cellphone_format": ["+227 9# ## ## ##", "+227 8# ## ## ##"],
        "cellphone_regex": [r"^\+227[89]\d{7}$"]
    },
    "NGA": {
        "zip_code_format": ["######", "#####"],
        "zip_code_regex": [r"^\d{6}$", r"^\d{5}$"],
        "telephone_format": ["+234 ## ### ####", "+234 ### ### ####", "+234 #### ### ####"],
        "telephone_regex": [r"^\+234\d{10}$"],
        "cellphone_format": ["+234 70# ### ####", "+234 80# ### ####", "+234 81# ### ####", "+234 90# ### ####", "+234 91# ### ####"],
        "cellphone_regex": [r"^\+234[7890]0\d{8}$"]
    },
    "MKD": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+389 ## ### ###"],
        "telephone_regex": [r"^\+389\d{8}$"],
        "cellphone_format": ["+389 7# ### ###", "+389 70 ### ###", "+389 71 ### ###", "+389 72 ### ###", "+389 75 ### ###", "+389 76 ### ###", "+389 77 ### ###", "+389 78 ### ###"],
        "cellphone_regex": [r"^\+3897[0125678]\d{6}$"]
    },
    "NOR": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+47 ## ## ## ##"],
        "telephone_regex": [r"^\+47\d{8}$"],
        "cellphone_format": ["+47 ## ## ## ##", "+47 4## ## ## ##", "+47 9## ## ## ##"],
        "cellphone_regex": [r"^\+47[49]?\d{7}$"]
    },
    # O
    "OMN": {
        "zip_code_format": ["###"],
        "zip_code_regex": [r"^\d{3}$"],
        "telephone_format": ["+968 #### ####"],
        "telephone_regex": [r"^\+968\d{8}$"],
        "cellphone_format": ["+968 9### ####", "+968 7### ####"],
        "cellphone_regex": [r"^\+968[79]\d{7}$"]
    },
    # P
    "PAK": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+92 ### #######"],
        "telephone_regex": [r"^\+92\d{9,10}$"],
        "cellphone_format": ["+92 3## #######"],
        "cellphone_regex": [r"^\+923\d{9}$"]
    },
    "PLW": {
        "zip_code_format": ["#####", "#####-####", "96940"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+680 ### ####"],
        "telephone_regex": [r"^\+680\d{7}$"],
        "cellphone_format": ["+680 ### ####", "+680 7## ####", "+680 8## ####"],
        "cellphone_regex": [r"^\+680[78]?\d{6}$"]
    },
    "PAN": {
        "zip_code_format": ["#####", "####"],
        "zip_code_regex": [r"^\d{4,5}$"],
        "telephone_format": ["+507 ####-####"],
        "telephone_regex": [r"^\+507\d{7,8}$"],
        "cellphone_format": ["+507 ####-####", "+507 6###-####"],
        "cellphone_regex": [r"^\+5076?\d{7}$"]
    },
    "PNG": {
        "zip_code_format": ["###"],
        "zip_code_regex": [r"^\d{3}$"],
        "telephone_format": ["+675 ### ####"],
        "telephone_regex": [r"^\+675\d{7}$"],
        "cellphone_format": ["+675 ### ####", "+675 7## ####", "+675 8## ####"],
        "cellphone_regex": [r"^\+675[78]?\d{6}$"]
    },
    "PRY": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+595 ### ######", "+595 ### ### ###"],
        "telephone_regex": [r"^\+595\d{9}$"],
        "cellphone_format": ["+595 97# ######", "+595 98# ######", "+595 99# ######"],
        "cellphone_regex": [r"^\+5959[789]\d{6}$"]
    },
    "PER": {
        "zip_code_format": ["#####", "#####-####", "#####-###"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$", r"^\d{5}-\d{3}$"],
        "telephone_format": ["+51 ### ### ###", "+51 ## ### ####", "+51 (###) ### ###"],
        "telephone_regex": [r"^\+51\d{9}$"],
        "cellphone_format": ["+51 9## ### ###"],
        "cellphone_regex": [r"^\+519\d{8}$"]
    },
    "PHL": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+63 ## ### ####", "+63 ### ### ####"],
        "telephone_regex": [r"^\+63\d{9,10}$"],
        "cellphone_format": ["+63 9## ### ####", "+63 9### ### ####"],
        "cellphone_regex": [r"^\+639\d{9,10}$"]
    },
    "POL": {
        "zip_code_format": ["##-###"],
        "zip_code_regex": [r"^\d{2}-?\d{3}$"],
        "telephone_format": ["+48 ## ### ## ##"],
        "telephone_regex": [r"^\+48\d{9}$"],
        "cellphone_format": ["+48 ### ### ###", "+48 ## ### ## ##", "+48 5## ### ###", "+48 6## ### ###", "+48 7## ### ###", "+48 8## ### ###"],
        "cellphone_regex": [r"^\+48[5678]?\d{8}$"]
    },
    "PRT": {
        "zip_code_format": ["####-###"],
        "zip_code_regex": [r"^\d{4}-?\d{3}$"],
        "telephone_format": ["+351 ### ### ###"],
        "telephone_regex": [r"^\+351\d{9}$"],
        "cellphone_format": ["+351 9## ### ###"],
        "cellphone_regex": [r"^\+3519\d{8}$"]
    },
    # Q
    "QAT": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+974 #### ####"],
        "telephone_regex": [r"^\+974\d{8}$"],
        "cellphone_format": ["+974 3### ####", "+974 5### ####", "+974 6### ####", "+974 7### ####"],
        "cellphone_regex": [r"^\+974[3567]\d{7}$"]
    },
    # R
    "ROU": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+40 ### ### ###"],
        "telephone_regex": [r"^\+40\d{9}$"],
        "cellphone_format": ["+40 7## ### ###"],
        "cellphone_regex": [r"^\+407\d{8}$"]
    },
    "RUS": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+7 ### ###-##-##", "+7 #### ##-##-##"],
        "telephone_regex": [r"^\+7\d{10}$"],
        "cellphone_format": ["+7 9## ###-##-##", "+7 9### ###-##-##"],
        "cellphone_regex": [r"^\+79\d{9}$"]
    },
    "RWA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+250 ### ### ###"],
        "telephone_regex": [r"^\+250\d{9}$"],
        "cellphone_format": ["+250 7## ### ###"],
        "cellphone_regex": [r"^\+2507\d{8}$"]
    },
    # S
    "KNA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+1 869 ### ####"],
        "telephone_regex": [r"^\+1869\d{7}$"],
        "cellphone_format": ["+1 869 ### ####", "+1 869 5## ####", "+1 869 6## ####", "+1 869 7## ####"],
        "cellphone_regex": [r"^\+1869\d{7}$"]
    },
    "LCA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+1 758 ### ####"],
        "telephone_regex": [r"^\+1758\d{7}$"],
        "cellphone_format": ["+1 758 ### ####", "+1 758 4## ####", "+1 758 5## ####", "+1 758 7## ####"],
        "cellphone_regex": [r"^\+1758\d{7}$"]
    },
    "VCT": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+1 784 ### ####"],
        "telephone_regex": [r"^\+1784\d{7}$"],
        "cellphone_format": ["+1 784 ### ####", "+1 784 4## ####", "+1 784 5## ####"],
        "cellphone_regex": [r"^\+1784\d{7}$"]
    },
    "WSM": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+685 ## ####"],
        "telephone_regex": [r"^\+685\d{5,7}$"],
        "cellphone_format": ["+685 7# ## ###", "+685 8# ## ###"],
        "cellphone_regex": [r"^\+685[78]\d{6}$"]
    },
    "SMR": {
        "zip_code_format": ["4789#"],
        "zip_code_regex": [r"^4789\d$"],
        "telephone_format": ["+378 #### ######"],
        "telephone_regex": [r"^\+378\d{6,10}$"],
        "cellphone_format": ["+378 3### ######", "+378 6### ######"],
        "cellphone_regex": [r"^\+378[36]\d{8}$"]
    },
    "STP": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+239 ## #####"],
        "telephone_regex": [r"^\+239\d{7}$"],
        "cellphone_format": ["+239 98#####", "+239 99#####"],
        "cellphone_regex": [r"^\+2399[89]\d{5}$"]
    },
    "SAU": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+966 ## #### ###", "+966 # #### ####"],
        "telephone_regex": [r"^\+966\d{9}$"],
        "cellphone_format": ["+966 5# #### ###", "+966 05# #### ####"],
        "cellphone_regex": [r"^\+9665\d{8}$"]
    },
    "SEN": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+221 ## ### ## ##"],
        "telephone_regex": [r"^\+221\d{9}$"],
        "cellphone_format": ["+221 7# ### ## ##"],
        "cellphone_regex": [r"^\+2217\d{8}$"]
    },
    "SRB": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+381 ## ### ####"],
        "telephone_regex": [r"^\+381\d{8,9}$"],
        "cellphone_format": ["+381 6# ### ####"],
        "cellphone_regex": [r"^\+3816\d{8}$"]
    },
    "SYC": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+248 ### ####"],
        "telephone_regex": [r"^\+248\d{7}$"],
        "cellphone_format": ["+248 ### ####", "+248 2## ####", "+248 4## ####", "+248 5## ####", "+248 7## ####"],
        "cellphone_regex": [r"^\+248[2457]?\d{6}$"]
    },
    "SLE": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+232 ## ######"],
        "telephone_regex": [r"^\+232\d{8}$"],
        "cellphone_format": ["+232 3# ######", "+232 7# ######", "+232 8# ######"],
        "cellphone_regex": [r"^\+232[378]\d{7}$"]
    },
    "SGP": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+65 #### ####"],
        "telephone_regex": [r"^\+65\d{8}$"],
        "cellphone_format": ["+65 #### ####", "+65 8### ####", "+65 9### ####"],
        "cellphone_regex": [r"^\+65[89]?\d{7}$"]
    },
    "SVK": {
        "zip_code_format": ["### ##", "#####"],
        "zip_code_regex": [r"^\d{3}\s?\d{2}$", r"^\d{5}$"],
        "telephone_format": ["+421 ### ### ###"],
        "telephone_regex": [r"^\+421\d{9}$"],
        "cellphone_format": ["+421 9## ### ###"],
        "cellphone_regex": [r"^\+4219\d{8}$"]
    },
    "SVN": {
        "zip_code_format": ["####", "SI-####"],
        "zip_code_regex": [r"^\d{4}$", r"^SI-\d{4}$"],
        "telephone_format": ["+386 ## ### ###"],
        "telephone_regex": [r"^\+386\d{8}$"],
        "cellphone_format": ["+386 4# ### ###", "+386 5# ### ###", "+386 3# ### ###"],
        "cellphone_regex": [r"^\+386[345]\d{7}$"]
    },
    "SLB": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+677 #####", "+677 ### ####"],
        "telephone_regex": [r"^\+677\d{5,7}$"],
        "cellphone_format": ["+677 7## ####", "+677 8## ####"],
        "cellphone_regex": [r"^\+677[78]\d{5}$"]
    },
    "SOM": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+252 ## ### ###", "+252 # ### ###"],
        "telephone_regex": [r"^\+252\d{7,8}$"],
        "cellphone_format": ["+252 6# ### ###", "+252 9# ### ###"],
        "cellphone_regex": [r"^\+252[69]\d{7}$"]
    },
    "ZAF": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+27 ## ### ####"],
        "telephone_regex": [r"^\+27\d{9}$"],
        "cellphone_format": ["+27 6## ### ###", "+27 7## ### ###", "+27 8## ### ###"],
        "cellphone_regex": [r"^\+27[678]\d{8}$"]
    },
    "KOR": {
        "zip_code_format": ["#####", "#####-###"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{3}$"],
        "telephone_format": ["+82 ##-####-####", "+82 ##-###-####", "+82-##-####-####"],
        "telephone_regex": [r"^\+82\d{9,10}$"],
        "cellphone_format": ["+82 1##-####-####", "+82 10-####-####"],
        "cellphone_regex": [r"^\+821[0]\d{8}$"]
    },
    "SSD": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+211 ## ### ####"],
        "telephone_regex": [r"^\+211\d{9}$"],
        "cellphone_format": ["+211 9# ### ####", "+211 92# ### ###", "+211 95# ### ###"],
        "cellphone_regex": [r"^\+2119[25]?\d{7}$"]
    },
    "ESP": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+34 ### ### ###", "+34 ## ### ## ##", "+34 ### ## ## ##"],
        "telephone_regex": [r"^\+34\d{9}$"],
        "cellphone_format": ["+34 6## ### ###", "+34 7## ### ###"],
        "cellphone_regex": [r"^\+34[67]\d{8}$"]
    },
    "LKA": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+94 ## ### ####"],
        "telephone_regex": [r"^\+94\d{9}$"],
        "cellphone_format": ["+94 7# ### ####"],
        "cellphone_regex": [r"^\+947\d{8}$"]
    },
    "SDN": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+249 ## ### ####"],
        "telephone_regex": [r"^\+249\d{9}$"],
        "cellphone_format": ["+249 9# ### ####", "+249 91# ### ###", "+249 92# ### ###", "+249 99# ### ###"],
        "cellphone_regex": [r"^\+2499[129]?\d{7}$"]
    },
    "SUR": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+597 ### ####"],
        "telephone_regex": [r"^\+597\d{7}$"],
        "cellphone_format": ["+597 ### ####", "+597 8## ####"],
        "cellphone_regex": [r"^\+5978?\d{6}$"]
    },
    "SWE": {
        "zip_code_format": ["### ##", "#####"],
        "zip_code_regex": [r"^\d{3}\s?\d{2}$", r"^\d{5}$"],
        "telephone_format": ["+46 ##-### ## ##", "+46 ###-## ## ##", "+46 ####-## ## ##"],
        "telephone_regex": [r"^\+46\d{8,10}$"],
        "cellphone_format": ["+46 70-### ## ##", "+46 72-### ## ##", "+46 73-### ## ##", "+46 76-### ## ##", "+46 79-### ## ##"],
        "cellphone_regex": [r"^\+467[023679]\d{7}$"]
    },
    "CHE": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+41 ## ### ## ##", "+41 ### ## ## ##"],
        "telephone_regex": [r"^\+41\d{9}$"],
        "cellphone_format": ["+41 7# ### ## ##", "+41 79 ### ## ##", "+41 76 ### ## ##", "+41 78 ### ## ##"],
        "cellphone_regex": [r"^\+41[78]\d{8}$"]
    },
    "SYR": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+963 ### ### ###"],
        "telephone_regex": [r"^\+963\d{9}$"],
        "cellphone_format": ["+963 9## ### ###"],
        "cellphone_regex": [r"^\+9639\d{8}$"]
    },
    # T
    "TJK": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+992 ### ## ####"],
        "telephone_regex": [r"^\+992\d{9}$"],
        "cellphone_format": ["+992 9## ## ####", "+992 92# ## ####", "+992 93# ## ####", "+992 95# ## ####", "+992 98# ## ####"],
        "cellphone_regex": [r"^\+9929[2358]?\d{7}$"]
    },
    "TZA": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+255 ### ### ###"],
        "telephone_regex": [r"^\+255\d{9}$"],
        "cellphone_format": ["+255 7## ### ###", "+255 6## ### ###"],
        "cellphone_regex": [r"^\+255[67]\d{8}$"]
    },
    "THA": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+66 ## ### ####", "+66 # ### ####"],
        "telephone_regex": [r"^\+66\d{8,9}$"],
        "cellphone_format": ["+66 8## ### ###", "+66 9## ### ###", "+66 6## ### ###"],
        "cellphone_regex": [r"^\+66[689]\d{8}$"]
    },
    "TLS": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+670 ### ####"],
        "telephone_regex": [r"^\+670\d{7}$"],
        "cellphone_format": ["+670 7## ####", "+670 77# ####", "+670 78# ####"],
        "cellphone_regex": [r"^\+6707[78]?\d{5}$"]
    },
    "TGO": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+228 ## ### ###"],
        "telephone_regex": [r"^\+228\d{8}$"],
        "cellphone_format": ["+228 9# ### ###", "+228 7# ### ###"],
        "cellphone_regex": [r"^\+228[79]\d{7}$"]
    },
    "TON": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+676 #####"],
        "telephone_regex": [r"^\+676\d{5,7}$"],
        "cellphone_format": ["+676 7####", "+676 8####", "+676 9####"],
        "cellphone_regex": [r"^\+676[789]\d{4}$"]
    },
    "TTO": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+1 868 ### ####"],
        "telephone_regex": [r"^\+1868\d{7}$"],
        "cellphone_format": ["+1 868 ### ####", "+1 868 3## ####", "+1 868 7## ####", "+1 868 4## ####"],
        "cellphone_regex": [r"^\+1868\d{7}$"]
    },
    "TUN": {
        "zip_code_format": ["####"],
        "zip_code_regex": [r"^\d{4}$"],
        "telephone_format": ["+216 ## ### ###"],
        "telephone_regex": [r"^\+216\d{8}$"],
        "cellphone_format": ["+216 2# ### ###", "+216 4# ### ###", "+216 5# ### ###", "+216 9# ### ###"],
        "cellphone_regex": [r"^\+216[2459]\d{7}$"]
    },
    "TUR": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+90 ### ### ## ##", "+90 ## ### ## ##", "+90 #### ### ## ##"],
        "telephone_regex": [r"^\+90\d{10}$"],
        "cellphone_format": ["+90 5## ### ## ##", "+90 50# ### ## ##", "+90 53# ### ## ##", "+90 54# ### ## ##", "+90 55# ### ## ##"],
        "cellphone_regex": [r"^\+905[0345]\d{8}$"]
    },
    "TKM": {
        "zip_code_format": ["######"],
        "zip_code_regex": [r"^\d{6}$"],
        "telephone_format": ["+993 # ### ####"],
        "telephone_regex": [r"^\+993\d{8}$"],
        "cellphone_format": ["+993 6# ### ####", "+993 65# ### ###", "+993 66# ### ###", "+993 71# ### ###"],
        "cellphone_regex": [r"^\+993[6]\d{7}$"]
    },
    "TUV": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+688 #####", "+688 ### ####"],
        "telephone_regex": [r"^\+688\d{5,6}$"],
        "cellphone_format": ["+688 7####", "+688 8####"],
        "cellphone_regex": [r"^\+688[78]\d{4}$"]
    },
    # U
    "UGA": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+256 ### ######"],
        "telephone_regex": [r"^\+256\d{9}$"],
        "cellphone_format": ["+256 7## ######", "+256 70# ######", "+256 71# ######", "+256 72# ######", "+256 75# ######", "+256 76# ######", "+256 77# ######", "+256 78# ######", "+256 79# ######"],
        "cellphone_regex": [r"^\+2567[01256789]\d{7}$"]
    },
    "UKR": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+380 ## ### ## ##"],
        "telephone_regex": [r"^\+380\d{9}$"],
        "cellphone_format": ["+380 67 ### ## ##", "+380 68 ### ## ##", "+380 96 ### ## ##", "+380 97 ### ## ##", "+380 98 ### ## ##", "+380 50 ### ## ##", "+380 63 ### ## ##", "+380 66 ### ## ##", "+380 95 ### ## ##", "+380 99 ### ## ##"],
        "cellphone_regex": [r"^\+380[569]\d{8}$"]
    },
    "ARE": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+971 # ### ####", "+971 ## ### ####"],
        "telephone_regex": [r"^\+971\d{8,9}$"],
        "cellphone_format": ["+971 5# ### ####", "+971 50# ### ###", "+971 52# ### ###", "+971 54# ### ###", "+971 55# ### ###", "+971 56# ### ###", "+971 58# ### ###"],
        "cellphone_regex": [r"^\+9715[02568]\d{7}$"]
    },
    "GBR": {
        "zip_code_format": ["AA# #AA", "A## #AA", "A#A #AA", "AA## #AA", "AN NAA", "ANN NAA", "AAN NAA", "AANN NAA", "AAA NAA"],
        "zip_code_regex": [r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$"],
        "telephone_format": ["+44 ## #### ####", "+44 ### ### ####", "+44 #### ######"],
        "telephone_regex": [r"^\+44\d{10,11}$"],
        "cellphone_format": ["+44 7### ######", "+44 7## ### ####"],
        "cellphone_regex": [r"^\+447\d{9}$"]
    },
    "USA": {
        "zip_code_format": ["#####", "#####-####"],
        "zip_code_regex": [r"^\d{5}$", r"^\d{5}-\d{4}$"],
        "telephone_format": ["+1 ### ###-####", "+1 (###) ###-####"],
        "telephone_regex": [r"^\+1\d{10}$"],
        "cellphone_format": ["+1 ### ###-####", "+1 (###) ###-####"],
        "cellphone_regex": [r"^\+1\d{10}$"]
    },
    "URY": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+598 #### ####"],
        "telephone_regex": [r"^\+598\d{8}$"],
        "cellphone_format": ["+598 9### ####", "+598 9# ### ####", "+598 8### ####"],
        "cellphone_regex": [r"^\+598[89]\d{7}$"]
    },
    "UZB": {
        "zip_code_format": ["######", "## ######"],
        "zip_code_regex": [r"^\d{6}$", r"^\d{2}\s\d{6}$"],
        "telephone_format": ["+998 ## ### ## ##"],
        "telephone_regex": [r"^\+998\d{9}$"],
        "cellphone_format": ["+998 9# ### ## ##", "+998 91# ## ## ##", "+998 93# ## ## ##", "+998 94# ## ## ##", "+998 97# ## ## ##", "+998 98# ## ## ##", "+998 99# ## ## ##"],
        "cellphone_regex": [r"^\+9989[134789]?\d{7}$"]
    },
    # V
    "VUT": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+678 #####", "+678 ### ####"],
        "telephone_regex": [r"^\+678\d{5,7}$"],
        "cellphone_format": ["+678 5####", "+678 7####", "+678 8####"],
        "cellphone_regex": [r"^\+678[578]\d{4}$"]
    },
    "VAT": {
        "zip_code_format": ["00120"],
        "zip_code_regex": [r"^00120$"],
        "telephone_format": ["+39 06 698#####"],
        "telephone_regex": [r"^\+3906698\d{5}$"],
        "cellphone_format": ["+39 3## ### ###"],
        "cellphone_regex": [r"^\+393\d{9}$"]
    },
    "VEN": {
        "zip_code_format": ["####", "####-A"],
        "zip_code_regex": [r"^\d{4}$", r"^\d{4}-[A-Z]$"],
        "telephone_format": ["+58 ###-###-####", "+58 ###-#######"],
        "telephone_regex": [r"^\+58\d{10}$"],
        "cellphone_format": ["+58 412-###-####", "+58 414-###-####", "+58 416-###-####", "+58 424-###-####", "+58 426-###-####"],
        "cellphone_regex": [r"^\+58[46][1246]\d{7}$"]
    },
    "VNM": {
        "zip_code_format": ["#####", "######", "#####-###"],
        "zip_code_regex": [r"^\d{5,6}$", r"^\d{5}-\d{3}$"],
        "telephone_format": ["+84 ## #### ###", "+84 ### #### ###"],
        "telephone_regex": [r"^\+84\d{9,10}$"],
        "cellphone_format": ["+84 9# #### ###", "+84 8# #### ###", "+84 7# #### ###", "+84 5# #### ###"],
        "cellphone_regex": [r"^\+84[5789]\d{8}$"]
    },
    # Y
    "YEM": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+967 ### ### ###", "+967 ## ### ###", "+967 # ### ###"],
        "telephone_regex": [r"^\+967\d{7,9}$"],
        "cellphone_format": ["+967 7## ### ###", "+967 71# ### ###", "+967 73# ### ###", "+967 77# ### ###", "+967 78# ### ###"],
        "cellphone_regex": [r"^\+9677[1378]?\d{7}$"]
    },
    # Z
    "ZMB": {
        "zip_code_format": ["#####"],
        "zip_code_regex": [r"^\d{5}$"],
        "telephone_format": ["+260 ## ### ####", "+260 ## ######", "+260 ## #######"],
        "telephone_regex": [r"^\+260\d{9}$"],
        "cellphone_format": ["+260 97# ### ###", "+260 95# ### ###", "+260 96# ### ###", "+260 76# ### ###", "+260 77# ### ###"],
        "cellphone_regex": [r"^\+260[79][567]\d{7}$"]
    },
    "ZWE": {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": ["+263 ## ######", "+263 ### ######", "+263 #### ######"],
        "telephone_regex": [r"^\+263\d{8,10}$"],
        "cellphone_format": ["+263 7# ######", "+263 71# ######", "+263 73# ######", "+263 77# ######", "+263 78# ######"],
        "cellphone_regex": [r"^\+2637[1378]?\d{7}$"]
    },
}


def get_country_formats(iso3_code):
    """
    Retorna os formatos para um país pelo código ISO3.
    Retorna dicionário vazio se país não encontrado.
    """
    return COUNTRY_FORMATS.get(iso3_code, {
        "zip_code_format": [None],
        "zip_code_regex": [None],
        "telephone_format": [None],
        "telephone_regex": [None],
        "cellphone_format": [None],
        "cellphone_regex": [None]
    })


def get_zip_code_info(iso3_code):
    """Retorna formatos e regex de zip code para o país."""
    formats = get_country_formats(iso3_code)
    return {
        "format": formats.get("zip_code_format", [None]),
        "regex": formats.get("zip_code_regex", [None])
    }


def get_telephone_info(iso3_code):
    """Retorna formatos e regex de telefone fixo para o país."""
    formats = get_country_formats(iso3_code)
    return {
        "format": formats.get("telephone_format", [None]),
        "regex": formats.get("telephone_regex", [None])
    }


def get_cellphone_info(iso3_code):
    """Retorna formatos e regex de celular para o país."""
    formats = get_country_formats(iso3_code)
    return {
        "format": formats.get("cellphone_format", [None]),
        "regex": formats.get("cellphone_regex", [None])
    }
