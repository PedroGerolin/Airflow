import time
import datetime 
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from airflow.models import Connection

class fisioVetDownloader:

    def __init__(self):
        default_directory = "/opt/airflow/files/FisioVet"
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
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
        self.conn = Connection.get_connection_from_secrets("fisioVet")
    
    def iniciar_navegador(self):
        self.driver.get(self.conn.host)

    def realizar_login(self):
        txt_login = self.driver.find_element(By.ID, "l_usu_var_email")
        txt_password = self.driver.find_element(By.ID, "l_usu_var_senha")
        btn_login = self.driver.find_element(By.ID, "btn_login")

        txt_login.send_keys(self.conn.login)
        txt_password.send_keys(self.conn.password)   
        btn_login.click()
        time.sleep(5)

    def enter_clients_page(self):
        lnk_clients = self.driver.find_element(By.LINK_TEXT, "Clientes")
        lnk_clients.click()
        time.sleep(2)

    def export_clients(self):
        btn_export = self.driver.find_element(By.ID, "p__btn_relatorio")
        btn_export.click()

        btn_csvAnimalCliente = self.driver.find_element(By.LINK_TEXT, "Exportar clientes e animais para CSV")
        btn_csvAnimalCliente.click()
        time.sleep(2)

    def enter_sales_page(self):
        self.driver.get("https://app.simples.vet/principal/venda/venda.php")
        time.sleep(2)

    def export_sales(self):
        dataInicial = '01/02/2025'#(datetime.date.today().replace(day=1)-timedelta(month=1)).replace(day=1).strftime("%d/%m/%Y")
        dataFinal = datetime.date.today().strftime("%d/%m/%Y")
        
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

    def enter_debts_page(self):
        self.driver.get("https://app.simples.vet/v3/financeiro/contas-a-pagar")
        time.sleep(2)
