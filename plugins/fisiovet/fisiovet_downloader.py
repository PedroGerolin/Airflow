import os
import glob
import time
import datetime
from datetime import timedelta
from httpcore import TimeoutException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from airflow.models import Connection

MESES_JANELA_VENDAS = 4


def inicio_da_janela(hoje: datetime.date, meses_atras: int) -> datetime.date:
    """Dia 1 do mês que fica `meses_atras` meses antes do mês de `hoje` (1 = dia 1 do mês passado)."""
    ano, mes = hoje.year, hoje.month - meses_atras
    while mes < 1:
        mes += 12
        ano -= 1
    return datetime.date(ano, mes, 1)


class fisioVetDownloader:

    def __init__(self):
        default_directory = "/opt/airflow/files/FisioVet"
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-infobars")
        options.add_argument(f"--download.default_directory={default_directory}")
        prefs = {
             "download.default_directory": default_directory,
             "savefile.default_directory": default_directory,
             "download.prompt_for_download": False,
             "profile.default_content_settings.popups": 0,
             "download.directory_upgrade": True,
             "safebrowsing.enabled": True,
             "profile.content_settings.exceptions.automatic_downloads": {"*": {"setting": 1}}
         }
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(options=options)
        # self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        #     "behavior": "allow",
        #     "downloadPath": default_directory
        # })
        self.conn = Connection.get_connection_from_secrets("fisioVet")
    
    def iniciar_navegador(self):
        self.driver.get(self.conn.host)

    def realizar_login(self):
        wait = WebDriverWait(self.driver, 20)
        
        txt_login = wait.until(EC.visibility_of_element_located((By.ID, "l_usu_var_email")))
        txt_password = self.driver.find_element(By.ID, "l_usu_var_senha")

        txt_login.clear()
        txt_login.send_keys(self.conn.login)
        txt_password.clear()
        txt_password.send_keys(self.conn.password)   
        txt_password.send_keys(Keys.ENTER)
        
        wait.until(EC.url_changes("https://app.simples.vet/login/login.php"))

    def enter_clients_page(self):
        wait = WebDriverWait(self.driver, 20)
        xpath_clientes = "//*[contains(text(), 'Clientes')] | //a[contains(., 'Clientes')]"
        
        lnk_clients = wait.until(EC.presence_of_element_located((By.XPATH, xpath_clientes)))
        self.driver.execute_script("arguments[0].click();", lnk_clients)

    def export_clients(self):
        wait = WebDriverWait(self.driver, 20)
        
        btn_export = wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[.//small[contains(text(), 'Relatórios')]]"))
        )
        self.driver.execute_script("arguments[0].click();", btn_export)

        btn_csvAnimalCliente = wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[.//p[contains(text(), 'Pessoas e animais')]]"))
        )
        self.driver.execute_script("arguments[0].click();", btn_csvAnimalCliente)

        btn_gerarPlaninha = wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[.//p[contains(text(), 'Gerar planilha')]]"))
        )
        self.driver.execute_script("arguments[0].click();", btn_gerarPlaninha)

        time.sleep(5)

    def enter_sales_page(self):
        self.driver.get("https://app.simples.vet/principal/venda/venda.php")
        time.sleep(2)

    def export_sales(self, data_inicial=None):
        # data_inicial (dd/mm/aaaa) só é usada na RECARGA COMPLETA (--conf sales_start_date da DAG);
        # na execução diária fica None e vale a janela padrão abaixo.
        # Janela diária: do dia 1 de MESES_JANELA_VENDAS meses atrás até hoje (ex.: hoje 21/10, 4 meses -> 01/06).
        # Cada partição diária do GCS só é regravada enquanto está na janela; depois vira uma fotografia congelada.
        # 4 meses (>= 120 dias em qualquer dia do ano) cobre os 120 dias que o dbt reprocessa, e a fila de cobrança carrega dívidas de
        # meses antigos: uma baixa dada fora da janela nunca chegaria ao warehouse (visto em 20/09/2026).
        dataInicial = inicio_da_janela(datetime.date.today(), MESES_JANELA_VENDAS).strftime("%d/%m/%Y")
        dataFinal = datetime.date.today().strftime("%d/%m/%Y")

        if data_inicial:
            datetime.datetime.strptime(data_inicial, "%d/%m/%Y")  # valida o formato (o valor vai num script JS)
            dataInicial = data_inicial

        txtData = self.driver.find_element(By.ID, "p__ven_dat_data")
        self.driver.execute_script("arguments[0].type='text';",txtData)
        self.driver.execute_script(f"arguments[0].value='{dataInicial}-{dataFinal}';",txtData)
        time.sleep(2)

        btn_relatorio = self.driver.find_element(By.ID, "p__btn_relatorio")
        btn_relatorio.click()
        time.sleep(2)

        btn_exportarcsv = self.driver.find_element(By.LINK_TEXT, "Exportar para CSV")
        btn_exportarcsv.click()
        time.sleep(2)

        # período de vários meses demora mais que o sleep fixo: espera o CSV terminar de baixar
        # (a transformação apaga o original depois de usar, então não há arquivo velho para confundir a espera)
        self.wait_for_download("Vendas.csv")

    def wait_for_download(self, file_name, timeout=600):
        folder = "/opt/airflow/files/FisioVet"
        path = os.path.join(folder, file_name)
        deadline = time.time() + timeout
        last_size = -1
        while time.time() < deadline:
            downloading = glob.glob(os.path.join(folder, "*.crdownload"))
            if os.path.isfile(path) and not downloading:
                size = os.path.getsize(path)
                if size == last_size:
                    return
                last_size = size
            time.sleep(3)
        raise TimeoutError(f"{file_name} não terminou de baixar em {timeout}s")

    def enter_debts_page(self):
        self.driver.get("https://app.simples.vet/v3/financeiro/contas-a-pagar")
        time.sleep(2)
