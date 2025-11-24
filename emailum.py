import pyodbc
import os
import base64
from datetime import datetime, date, timedelta
import pytz
from email.message import EmailMessage
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Configurações do Banco de Dados
DB_CONFIG = {
    'server': '134.65.243.58,1433',
    'database': 'SANKHYA_PROD',
    'username': 'sankhya',
    'password': 'h4xg7s3f'
}

# Configurações do Gmail
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
SENDER_EMAIL = 'drive.ti@institutodds.org'
NEIDE_EMAIL = 'neide.silva@institutodds.org.br'

# Nome do script (pega o nome do arquivo atual)
SCRIPT_NAME = os.path.basename(__file__)

class EmailSender:
    def __init__(self):
        self.creds = None
        self.service = None

    def authenticate(self):
        """Autentica com a API do Gmail usando OAuth2"""
        if os.path.exists(TOKEN_FILE):
            self.creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                self.creds = flow.run_local_server(port=0)

            with open(TOKEN_FILE, 'w') as token:
                token.write(self.creds.to_json())

        self.service = build('gmail', 'v1', credentials=self.creds)

    def send_email(self, to_email, cc_emails, subject, body_html):
        """Envia um e-mail usando a API do Gmail"""
        try:
            msg = EmailMessage()
            msg['From'] = SENDER_EMAIL
            msg['To'] = to_email
            if cc_emails:
                msg['Cc'] = ', '.join(cc_emails)
            msg['Subject'] = subject
            msg.set_content(body_html, subtype='html')

            raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            body = {'raw': raw_message}
            self.service.users().messages().send(userId='me', body=body).execute()
            return True
        except Exception as e:
            print(f"Erro ao enviar e-mail para {to_email}: {str(e)}")
            return False

class DatabaseManager:
    def __init__(self):
        self.conn = None

    def connect(self):
        """Conecta ao banco de dados SQL Server"""
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']}"
        )
        try:
            self.conn = pyodbc.connect(conn_str)
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {str(e)}")
            return None
        return self.conn.cursor()

    def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                print(f"Erro ao fechar conexão com o banco de dados: {str(e)}")

    def log_execution(self, status, error_message=None):
        """Registra a execução do script na tabela de log"""
        try:
            now = datetime.now()
            chave = f"{now.strftime('%Y%m%d%H%M%S')}-{SCRIPT_NAME}"
            data = now.date()
            hora = now.time()
            email = "marcus.costa@institutodds.org"
            
            # Cria uma nova conexão para o log para evitar problemas com a conexão principal
            temp_conn = None
            try:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={DB_CONFIG['server']};"
                    f"DATABASE={DB_CONFIG['database']};"
                    f"UID={DB_CONFIG['username']};"
                    f"PWD={DB_CONFIG['password']}"
                )
                temp_conn = pyodbc.connect(conn_str)
                cursor = temp_conn.cursor()
                
                query = """
                INSERT INTO log_automacoes 
                (chave, nome_script, data, hora, status, email, mensagem_erro)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(query, chave, SCRIPT_NAME, data, hora, status, email, error_message)
                temp_conn.commit()
                print(f"Log registrado no banco de dados: {status}")
            finally:
                if temp_conn:
                    temp_conn.close()
        except Exception as e:
            print(f"Erro ao registrar log no banco de dados: {str(e)}")

    def get_pending_rc(self):
        """Obtém as requisições de compra pendentes do banco de dados"""
        query = """
        


 SELECT
        CUS.DESCRCENCUS AS CENTRO_CUSTO
        ,NAT.DESCRNAT as NATUREZA
        ,PAR.NOMEPARC
        ,USU.NOMEUSU AS EXECUTIVO_CR
        ,USU.EMAIL AS email_do_executivo_responsavel
        ,TB.*
        FROM
        (
        SELECT DISTINCT
        PRN.CODPRN
        ,PRC.NOME AS PROCESSO
        ,TAR.IDINSTPRN as NUMERO_RC
        ,ELE.NOME AS TAREFA_ATUAL
        ,PRN.DHINCLUSAO as DATA_SOLICITACAO
        ,TAR.DHCRIACAO as INICIO_TAREFA
        ,TAR.DHVENCIMENTO as PRAZO_TAREFA
        ,USU1.NOMEUSU AS SOLICITANTE
        ,USU1.EMAIL AS EMAIL_DO_SOLICITANTE
        ,USU.NOMEUSU AS RESPONSAVEL_TAREFA
        ,case when f.situacao= 0 then 'Responsavel_inativo' else ' Responsavel_ativo' end status_responsavel,
        case when f_dois.situacao= 0 then 'solicitante_inativo' else ' solicitante_ativo' end status_solicitante,
        USU.EMAIL AS EMAIL_DO_RESPONSAVEL
        ,(CASE WHEN USU.DTLIMACESSO IS NOT NULL THEN 'Funcionario_demitido' ELSE 'ATIVO' END) status_func

        ,(ISNULL((SELECT
        MAX(CODCENCUS)
        FROM AD_FATCON 
        WHERE AD_FATCON.IDINSTPRN = TAR.IDINSTPRN),(SELECT TOP 1
        CODCENCUS
        FROM AD_REQUISICAOCOMPRA  
        WHERE AD_REQUISICAOCOMPRA.IDINSTPRN = TAR.IDINSTPRN))) AS CODCENCUS

        ,(ISNULL((SELECT TOP 1
        CODNAT
        FROM AD_FATCON 
        WHERE AD_FATCON.IDINSTPRN = TAR.IDINSTPRN),(SELECT TOP 1
        CODNAT
        FROM AD_REQUISICAOCOMPRA 
        WHERE AD_REQUISICAOCOMPRA.IDINSTPRN = TAR.IDINSTPRN))) AS CODNAT


        ,(ISNULL((SELECT TOP 1
        CODPARC
        FROM AD_FATCON 
        WHERE AD_FATCON.IDINSTPRN = TAR.IDINSTPRN),(SELECT TOP 1
        CODPARC
        FROM AD_REQUISICAOCOMPRA 
        WHERE AD_REQUISICAOCOMPRA.IDINSTPRN = TAR.IDINSTPRN))) AS CODPARC

        ,DATEDIFF(DAY, PRN.DHINCLUSAO, GETDATE()) DIAS_ABERTO_REQUISICAO
        ,DATEDIFF(DAY, TAR.DHCRIACAO, GETDATE()) DIAS_ABERTO
        ,DATEDIFF(DAY, TAR.DHVENCIMENTO, GETDATE()) DIAS_EM_ATRASO
         FROM TWFITAR TAR
         INNER JOIN TWFITAR_ELEMENTO ELE on ELE.IDINSTPRN = TAR.IDINSTPRN and ELE.IDELEMENTO = TAR.IDELEMENTO
         INNER JOIN TWFIPRN PRN ON PRN.IDINSTPRN = TAR.IDINSTPRN
         INNER JOIN TWFPRN PRC ON PRC.CODPRN = PRN.CODPRN AND PRC.VERSAO = PRN.VERSAO
         LEFT JOIN TSIUSU USU ON USU.CODUSU = TAR.CODUSUDONO 
         LEFT JOIN TSIUSU USU1 ON USU1.CODUSU = PRN.CODUSUINC
         left join tfpfun f on f.cpf = usu.cpf
         left join tfpfun f_dois on f_dois.cpf = usu1.cpf
         WHERE PRN.CODPRN IN (3,9) 
         AND PRN.SITUACAOEXEC = 'I'
         AND TAR.DHCONCLUSAO IS NULL
         AND TAR.DHVENCIMENTO < GETDATE()
         AND ELE.NOME <> 'Registrar Itens'
         AND PRN.DHINCLUSAO >= '2024-07-01'

        ) TB
        INNER JOIN TSICUS CUS ON CUS.CODCENCUS = TB.CODCENCUS
        INNER JOIN TGFNAT NAT ON NAT.CODNAT = TB.CODNAT
        INNER JOIN TGFPAR PAR ON PAR.CODPARC = TB.CODPARC
        INNER JOIN TSIUSU USU ON USU.CODUSU = CUS.CODUSURESP
        """
        cursor = self.connect()
        if not cursor:
            return []
            
        try:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            # Converter para lista de dicionários
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            return result
        except Exception as e:
            print(f"Erro ao buscar requisições pendentes: {str(e)}")
            return []
        finally:
            try:
                cursor.close()
            except:
                pass

def is_business_day(date):
    """Verifica se a data é um dia útil (segunda a sexta)"""
    return date.weekday() < 5

def is_monday(date):
    """Verifica se é segunda-feira"""
    return date.weekday() == 0

def is_thursday(date):
    """Verifica se é quinta-feira"""
    return date.weekday() == 3

def group_data(rows):
    """Agrupa os dados por responsável, por solicitante e por executivo (para funcionários demitidos)"""
    responsaveis = {}
    solicitantes = {}
    executivos_func_demitido = {}
    
    for row in rows:
        # Agrupar por responsável
        if row['EMAIL_DO_RESPONSAVEL']:
            if row['EMAIL_DO_RESPONSAVEL'] not in responsaveis:
                responsaveis[row['EMAIL_DO_RESPONSAVEL']] = {
                    'nome': row['RESPONSAVEL_TAREFA'],
                    'items': []
                }
            responsaveis[row['EMAIL_DO_RESPONSAVEL']]['items'].append(row)
        
        # Agrupar por solicitante
        if row['EMAIL_DO_SOLICITANTE']:
            if row['EMAIL_DO_SOLICITANTE'] not in solicitantes:
                solicitantes[row['EMAIL_DO_SOLICITANTE']] = {
                    'nome': row['SOLICITANTE'],
                    'items': []
                }
            solicitantes[row['EMAIL_DO_SOLICITANTE']]['items'].append(row)
        
        # Agrupar por executivo para funcionários demitidos
        if row['status_func'] == 'Funcionario_demitido' and row['email_do_executivo_responsavel']:
            if row['email_do_executivo_responsavel'] not in executivos_func_demitido:
                executivos_func_demitido[row['email_do_executivo_responsavel']] = {
                    'nome': row['EXECUTIVO_CR'],
                    'items': []
                }
            executivos_func_demitido[row['email_do_executivo_responsavel']]['items'].append(row)
    
    return responsaveis, solicitantes, executivos_func_demitido

def generate_responsible_email(responsible_name, rc_items):
    """Gera o corpo do e-mail HTML para os responsáveis"""
    items_html = "\n".join(
        f"""
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['NUMERO_RC']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['PROCESSO']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['CENTRO_CUSTO']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['TAREFA_ATUAL']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['DATA_SOLICITACAO'].strftime('%d/%m/%Y')}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['PRAZO_TAREFA'].strftime('%d/%m/%Y')}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['DIAS_EM_ATRASO']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['SOLICITANTE']}</td>
        </tr>
        """
        for item in rc_items
    )
    
    return f"""
    <html>
        <body>
            <p>Olá {responsible_name.strip()}, tudo bom?</p>
            <p>Você possui uma ou mais tarefas em atraso.</p>
            <p>Segue a listagem das requisições e a tarefa que está pendente:</p>
            
            <table style="border-collapse: collapse; width: 100%; margin-top: 15px; margin-bottom: 15px;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Número da RC</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Processo</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Centro de Custo</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Tarefa Atual</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Data da Solicitação</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Prazo da Tarefa</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Dias em Atraso</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Solicitante</th>
                    </tr>
                </thead>
                <tbody>
                    {items_html}
                </tbody>
            </table>
            
            <p>Está é uma mensagem automática , sendo assim não faz-se necessário a mensagem nesse email ,sobre qualquer dúvida ou comunicado, envie mensagens para os setores responsáveis.</p>
            <p>Atenciosamente,<br>Equipe de TI</p>
        </body>
    </html>
    """

def generate_solicitante_email(solicitante_name, rc_items):
    """Gera o corpo do e-mail HTML para os solicitantes"""
    items_html = "\n".join(
        f"""
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['NUMERO_RC']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['PROCESSO']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['TAREFA_ATUAL']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['RESPONSAVEL_TAREFA']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['CENTRO_CUSTO']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['DATA_SOLICITACAO'].strftime('%d/%m/%Y')}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['PRAZO_TAREFA'].strftime('%d/%m/%Y')}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['DIAS_EM_ATRASO']}</td>
        </tr>
        """
        for item in rc_items
    )
    
    return f"""
    <html>
        <body>
            <p>Olá {solicitante_name.strip()}, tudo bom?</p>
            <p>Segue a listagem das tarefas solicitadas por você que estão em atraso:</p>
            
            <table style="border-collapse: collapse; width: 100%; margin-top: 15px; margin-bottom: 15px;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">RC</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Processo</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Tarefa atual</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Responsável</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Centro de Custo</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Data da Solicitação</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Prazo da Tarefa</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Dias em Atraso</th>
                    </tr>
                </thead>
                <tbody>
                    {items_html}
                </tbody>
            </table>
            
            <p>Essa é uma mensagem automática programada para avisos de RCs em atraso,sendo assim não faz-se necessário responder nesse email. 
            Para qualquer comunicado, entre em contato com os setores responsáveis. 
            Em caso de necessidade de alteração ou exclusão da REQUISIÇÃO entre em contato com o TI pelo sistema de chamados Gestão X</p>
            <p>Atenciosamente,<br>Equipe de TI</p>
        </body>
    </html>
    """

def generate_executivo_email(executivo_name, rc_items):
    """Gera o corpo do e-mail HTML para os executivos sobre funcionários demitidos"""
    items_html = "\n".join(
        f"""
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['NUMERO_RC']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['PROCESSO']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['TAREFA_ATUAL']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['RESPONSAVEL_TAREFA']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['DIAS_EM_ATRASO']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{item['SOLICITANTE']}</td>
        </tr>
        """
        for item in rc_items
    )
    
    return f"""
    <html>
        <body>
            <p>Olá {executivo_name.strip()}, tudo bem?</p>
            <p>As REQUISIÇÕES abaixo estão paradas na tarefa com funcionário que não está mais ativo:</p>
            
            <table style="border-collapse: collapse; width: 100%; margin-top: 15px; margin-bottom: 15px;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Número da RC</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Processo</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Tarefa Atual</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Funcionário Inativo</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Dias em Atraso</th>
                        <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Solicitante</th>
                    </tr>
                </thead>
                <tbody>
                    {items_html}
                </tbody>
            </table>
            
            <p>Se possível, por favor, solicite a atribuição de outra pessoa do setor nessa tarefa para que a tarefa tenha continuidade.</p>
            <p>Atenciosamente,<br>Equipe de TI</p>
        </body>
    </html>
    """

def main():
    hoje = date.today()
    db_manager = DatabaseManager()
    
    try:
        # Verifica se é dia útil
        if not is_business_day(hoje):
            msg = f"Hoje é {hoje.strftime('%A')}. Os e-mails só são enviados em dias úteis."
            print(msg)
            db_manager.log_execution("Sucesso", msg)
            return

        # Autentica no Gmail
        email_sender = EmailSender()
        try:
            email_sender.authenticate()
        except Exception as e:
            error_msg = f"Erro na autenticação com o Gmail: {str(e)}"
            print(error_msg)
            db_manager.log_execution("Script mal sucedido", error_msg)
            return

        # Obtém as requisições pendentes
        try:
            rows = db_manager.get_pending_rc()
            responsaveis, solicitantes, executivos_func_demitido = group_data(rows)
            print(f"Encontradas {len(rows)} requisições pendentes")
        except Exception as e:
            error_msg = f"Erro ao buscar requisições pendentes: {str(e)}"
            print(error_msg)
            db_manager.log_execution("Script mal sucedido", error_msg)
            return

        # Processa cada responsável (todos os dias úteis)
        email_errors = []
        emails_enviados = 0
        
        try:
            for email, data in responsaveis.items():
                # Verifica se é EPI E UNIFORME para incluir Neide no CC
                cc_emails = []
                epi_items = [item for item in data['items'] if item['NATUREZA'] == 'EPI E UNIFORME']
                if epi_items:
                    cc_emails.append(NEIDE_EMAIL)
                
                subject = "::::::Requisições em Atraso::::::"
                body = generate_responsible_email(data['nome'], data['items'])
                
                if not body:
                    print(f"Nenhuma RC pendente para notificar o responsável {email}.")
                    continue
                
                if email_sender.send_email(email, cc_emails, subject, body):
                    print(f"E-mail enviado com sucesso para responsável: {email} (CC: {', '.join(cc_emails) if cc_emails else 'Nenhum'})")
                    emails_enviados += 1
                else:
                    error_msg = f"Falha ao enviar e-mail para responsável: {email}"
                    print(error_msg)
                    email_errors.append(error_msg)

            # Processa cada solicitante (somente às segundas-feiras)
            if is_monday(hoje):
                for email, data in solicitantes.items():
                    subject = "Tarefas de compras solicitadas por você que estão em atraso"
                    body = generate_solicitante_email(data['nome'], data['items'])
                    
                    if not body:
                        print(f"Nenhuma RC pendente para notificar o solicitante {email}.")
                        continue
                    
                    if email_sender.send_email(email, [], subject, body):
                        print(f"E-mail enviado com sucesso para solicitante: {email}")
                        emails_enviados += 1
                    else:
                        error_msg = f"Falha ao enviar e-mail para solicitante: {email}"
                        print(error_msg)
                        email_errors.append(error_msg)
            else:
                msg = "Hoje não é segunda-feira - pulando envio para solicitantes"
                print(msg)

            # Processa cada executivo para casos de funcionários demitidos (segunda e quinta)
            if is_monday(hoje) or is_thursday(hoje):
                for email, data in executivos_func_demitido.items():
                    subject = "Processo de compra parado em funcionário Inativo"
                    body = generate_executivo_email(data['nome'], data['items'])
                    
                    if not body:
                        print(f"Nenhum caso de funcionário demitido para notificar o executivo {email}.")
                        continue
                    
                    if email_sender.send_email(email, [], subject, body):
                        print(f"E-mail enviado com sucesso para executivo sobre funcionário demitido: {email}")
                        emails_enviados += 1
                    else:
                        error_msg = f"Falha ao enviar e-mail para executivo sobre funcionário demitido: {email}"
                        print(error_msg)
                        email_errors.append(error_msg)
            else:
                msg = "Hoje não é segunda nem quinta-feira - pulando envio para executivos"
                print(msg)

            # Registra o resultado da execução
            if email_errors:
                error_msg = f"Processamento concluído com erros. E-mails enviados: {emails_enviados}. Erros: {len(email_errors)}"
                print(error_msg)
                db_manager.log_execution("Parcialmente sucedido", "\n".join(email_errors))
            else:
                success_msg = f"Processamento concluído com sucesso. Total de e-mails enviados: {emails_enviados}"
                print(success_msg)
                db_manager.log_execution("Sucesso", success_msg)
                
        except Exception as e:
            error_msg = f"Erro durante o envio de e-mails: {str(e)}"
            print(error_msg)
            db_manager.log_execution("Script mal sucedido", error_msg)
            
    except Exception as e:
        error_msg = f"Erro crítico: {str(e)}"
        print(error_msg)
        try:
            db_manager.log_execution("Script mal sucedido", error_msg)
        except:
            print("Falha ao registrar o erro no banco de dados")
    finally:
        db_manager.close()

if __name__ == "__main__":
    main()