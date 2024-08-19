# Importando as bibliotecas necessárias
from babel.dates import format_datetime
import fdb
import smtplib
import datetime
import locale
import pytz
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Site para hospedar logo do cliente: https://pt-br.imgbb.com/

# Configurações gerais
MetodoWorkFlow = 'Producao'  # Homologacao ou Producao
# {Homologacao} Envia e-mail apenas para o cliente, para testar o workflow
# {Producao} Envia e-mail para os clientes

# Parâmetros de lançamento de e-mail
FiliaisParticipantes = '1,3'  # Preencher separadas por vírgula
DataInicio = '01.01.2024'

# Parâmetros do email - 1 (um) email contém 1 (um) cliente com diversos lançamentos
QtdDiasAntesVencer = 3
QtdDiasDepoisVencer = 3
QtdEmailPorVez = 3
QtdEmailPorVezAntesVencer = 2
QtdEmailPorVezDiaVencimento = 2
NomeEmpresa = 'Dubrasil Soluções'
smtp_server = 'smtp-mail.outlook.com'
smtp_port = 587
smtp_username = 'teste@gmail.com'
smtp_password = '123465'


# Parâmetros do layout do email
CaminhoLogo = ''
AlturaLogo = '169'
LarguraLogo = '250'
EmailContato = 'teste@gmail.com'
TelefoneContato = '(34) 3332-8500'
FraseRepresentante = 'Enviado por TGA Sistemas - www.dubrasilsolucoes.com.br - (34) 3322.8500'
CorEmail = '#1A1B32'
DepartamentoEmpresa = 'Depto. Financeiro'
CidadeEmpresa = 'Uberaba - MG'


# Enviar para qual email
EnviaQualEmail = 'EmailGeral'
# Preencher esse campo com uma das opções abaixo
# {EmailGeral} Campo 'Email' na aba 1 - identificação
# {EmailFiscal} Campo 'E-mail fiscal (NF-e)' na aba 6 - Tributação
# {CampoComplementar} Criar campo complementar do cliente com nome de EMAILFINAN

EnviaCopiaEmpresa = 'Sim'
# Preencher esse campo com uma das opções abaixo
# {Sim} Envia cópia para o e-mail configurado no parâmetro 'EmailContato' para o cliente ter controle dos emails enviados
# {Nao} Não envia cópia, enviando então o e-mail apenas para o cliente.

# -------------------------------------------------------------------------
# Tabelas auxiliares
# -------------------------------------------------------------------------
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
# Dados do banco de dados
database_path = 'c:\\tga\\dados\\TGA.FDB'
user = 'SYSDBA'
password = 'masterkey'
server_ip = '192.168.xxx.xxx' 
port = 3050  # A porta padrão do Firebird é 3050'

# Conectar ao banco de dados remoto
dsn = f'{server_ip}/{port}:{database_path}'
try:
    con = fdb.connect(dsn=dsn, user=user, password=password)
except fdb.fbcore.DatabaseError as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    # Encerre o script ou lide com o erro de outra forma, dependendo dos requisitos do seu aplicativo
    exit(1)

# Selecionar o campo de e-mail apropriado com base na configuração
if EnviaQualEmail == 'CampoComplementar':
    EmailSQL = 'fcfocompl.emailfinan'
elif EnviaQualEmail == 'EmailGeral':
    EmailSQL = 'fcfo.email'
elif EnviaQualEmail == 'EmailFiscal':
    EmailSQL = 'fcfo.emailfiscal'


def criar_tabela_dbs_email_vencido():
    # Criar um cursor para executar consultas
    cursor = con.cursor()

    # Verificar se a tabela DBS_EMAIL_VENCIDO já existe, senão, criá-la
    cursor.execute(
        "SELECT COUNT(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE UPPER(RDB$RELATION_NAME) = UPPER('DBS_EMAIL_VENCIDO')")
    resultado = cursor.fetchone()
    valor_str = resultado[0] if resultado else 0

    if int(valor_str) == 0:
        cursor.execute("""
            CREATE TABLE DBS_EMAIL_VENCIDO (
                IDLAN INTEGER NOT NULL,
                DataHora TIMESTAMP
            )
        """)
        con.commit()

    # Fechar o cursor
    cursor.close()


def criar_tabela_dbs_email_antes_vencimento():

    cursor = con.cursor()

    cursor.execute(
        "select count(rdb$relation_name) from rdb$relations where upper(rdb$relation_name) = upper('AA_LAN_EMAIL_AVENCER')")
    resultado = cursor.fetchone()
    valor_str = resultado[0] if resultado else 0

    if int(valor_str) == 0:
        cursor.execute("""
            CREATE TABLE AA_LAN_EMAIL_AVENCER (
                IDLAN INTEGER NOT NULL,
                DataHora TIMESTAMP
            )
        """)
        con.commit()

    cursor.close()


def criar_tabela_dbs_email_dia_vencimento():

    cursor = con.cursor()

    cursor.execute(
        "select count(rdb$relation_name) from rdb$relations where upper(rdb$relation_name) = upper('AA_LAN_EMAIL_DIA_VENCIMENTO')")
    resultado = cursor.fetchone()
    valor_str = resultado[0] if resultado else 0

    if int(valor_str) == 0:
        cursor.execute("""
                CREATE TABLE AA_LAN_EMAIL_DIA_VENCIMENTO (
                    IDLAN INTEGER NOT NULL,
                    DataHora TIMESTAMP
                )
            """)
        con.commit()

    cursor.close()


def obter_destinatarios(resultados):
    destinatarios = []
    cursor = con.cursor()

    # Iterar sobre os resultados para obter os destinatários
    for resultado in resultados:
        codcfo, nomefantasia, email_do_cliente = resultado

        if MetodoWorkFlow == 'Homologacao':
            # Se o modo de workflow for 'Homologacao', enviar apenas para o EmailContato
            destinatarios.append(EmailContato)
        else:
            qryEmailContato = cursor

            # Consultar o e-mail de contato financeiro para o cliente
            qryEmailContato.execute(f"""
                SELECT fcfocontatopj.email
                FROM fcfocontatopj
                WHERE UPPER(fcfocontatopj.departamento) = 'FINANCEIRO' AND
                        POSITION('@', fcfocontatopj.email) > 0 AND
                        fcfocontatopj.codcfo = '{codcfo}'
            """)

            if qryEmailContato.rowcount > 0:
                emails_cliente = set()
                for row in qryEmailContato.fetchall():
                    lista_emails = row['email'].split(';')
                    emails_cliente.update(email.strip()
                                          for email in lista_emails if email.strip())

                destinatarios.extend(emails_cliente)
            else:
                destinatarios.append(email_do_cliente)

    # Adicionar o e-mail da empresa, se necessário
    if EnviaCopiaEmpresa == 'Sim':
        destinatarios.append(EmailContato)

    # Separar cada endereço de e-mail individualmente
    destinatarios = [email.strip(
    ) for destinatario in destinatarios for email in destinatario.split(';')]

    return destinatarios


def enviar_email_filial1(destinatarios, corpo_email_filial1):
    print(f"Destinatários antes do envio: {destinatarios}")

    mensagem = MIMEMultipart()
    mensagem['Subject'] = 'Posição Financeira - Títulos em aberto'
    mensagem['From'] = smtp_username

    corpo = MIMEText(corpo_email_filial1, 'html')
    mensagem.attach(corpo)

    destinatarios_str = ';'.join(destinatarios)
    mensagem['To'] = destinatarios_str.replace(';', ',')

# Quando for Gmail usar essa configuração
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    server.quit()

  #Quando for outro servidor usar essa configuração
    #server = smtplib.SMTP_SSL(smtp_server, smtp_port)
    #server.login(smtp_username, smtp_password)
    #server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    #server.quit()

    # Imprimir informações sobre o e-mail enviado
    #print(f"Enviando e-mail para: {destinatarios}")
    #print(f"Assunto: {mensagem['Subject']}")
    #print(f"Corpo:\n{corpo_email_filial1}")

def enviar_email_filial3(destinatarios, corpo_email_filial3):
    print(f"Destinatários antes do envio: {destinatarios}")

    mensagem = MIMEMultipart()
    mensagem['Subject'] = 'Posição Financeira - Títulos em aberto'
    mensagem['From'] = smtp_username

    corpo = MIMEText(corpo_email_filial3, 'html')
    mensagem.attach(corpo)

    destinatarios_str = ';'.join(destinatarios)
    mensagem['To'] = destinatarios_str.replace(';', ',')

# Quando for Gmail usar essa configuração
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    server.quit()

  #Quando for outro servidor usar essa configuração
    #server = smtplib.SMTP_SSL(smtp_server, smtp_port)
    #server.login(smtp_username, smtp_password)
    #server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    #server.quit()

def enviar_email_antes_vencimento_filial1(destinatarios, corpo_email_antes_vencimento_filial1):

    mensagem = MIMEMultipart()
    mensagem['Subject'] = 'Posição Financeira - A vencer'
    mensagem['From'] = smtp_username

    corpo = MIMEText(corpo_email_antes_vencimento_filial1, 'html')
    mensagem.attach(corpo)

    destinatarios_str = ';'.join(destinatarios)
    mensagem['To'] = destinatarios_str.replace(';', ',')

#Quando for Gmail usar essa configuração
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    server.quit()

 # Quando for outro servidor usar essa configuração
    #server = smtplib.SMTP_SSL(smtp_server, smtp_port)
    #server.login(smtp_username, smtp_password)
    #server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    #server.quit()

def enviar_email_antes_vencimento_filial3(destinatarios, corpo_email_antes_vencimento_filial3):

    mensagem = MIMEMultipart()
    mensagem['Subject'] = 'Posição Financeira - A vencer'
    mensagem['From'] = smtp_username

    corpo = MIMEText(corpo_email_antes_vencimento_filial3, 'html')
    mensagem.attach(corpo)

    destinatarios_str = ';'.join(destinatarios)
    mensagem['To'] = destinatarios_str.replace(';', ',')

#Quando for Gmail usar essa configuração
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    server.quit()

 # Quando for outro servidor usar essa configuração
    #server = smtplib.SMTP_SSL(smtp_server, smtp_port)
    #server.login(smtp_username, smtp_password)
    #server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    #server.quit()


def enviar_email_dia_vencimento_filial1(destinatarios, corpo_email_dia_vencimento_filial1):

    mensagem = MIMEMultipart()
    mensagem['Subject'] = 'Posição Financeira - Lembrete de Vencimento'
    mensagem['From'] = smtp_username

    corpo = MIMEText(corpo_email_dia_vencimento_filial1, 'html')
    mensagem.attach(corpo)

    destinatarios_str = ';'.join(destinatarios)
    mensagem['To'] = destinatarios_str.replace(';', ',')

# Quando for Gmail usar essa configuração
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    server.quit()

 # Quando for outro servidor usar essa configuração
    # server = smtplib.SMTP_SSL(smtp_server, smtp_port)
    # server.login(smtp_username, smtp_password)
    # server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    # server.quit()

def enviar_email_dia_vencimento_filial3(destinatarios, corpo_email_dia_vencimento_filial3):

    mensagem = MIMEMultipart()
    mensagem['Subject'] = 'Posição Financeira - Lembrete de Vencimento'
    mensagem['From'] = smtp_username

    corpo = MIMEText(corpo_email_dia_vencimento_filial3, 'html')
    mensagem.attach(corpo)

    destinatarios_str = ';'.join(destinatarios)
    mensagem['To'] = destinatarios_str.replace(';', ',')

# Quando for Gmail usar essa configuração
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    server.quit()

 # Quando for outro servidor usar essa configuração
    # server = smtplib.SMTP_SSL(smtp_server, smtp_port)
    # server.login(smtp_username, smtp_password)
    # server.sendmail(smtp_username, destinatarios, mensagem.as_string())
    # server.quit()


def obter_data_hora_envio():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    formato_data_hora = "EEEE, d 'de' MMMM 'de' y - HH:mm"
    data_hora_envio = format_datetime(agora, formato_data_hora, locale='pt_BR')
    return data_hora_envio


# Exemplo de uso
data_hora_envio = obter_data_hora_envio()
print("Data e hora de envio:", data_hora_envio)


def buscar_clientes_vencidos_filial1():
    cursor = con.cursor()
    consulta_sql_cliente_filial1 = f"""
        SELECT FIRST {QtdEmailPorVez} DISTINCT fcfo.codcfo, fcfo.nomefantasia, {EmailSQL} AS email
        FROM flan
        INNER JOIN fcfo ON (fcfo.codcfo = flan.codcfo)
        LEFT JOIN dbs_email_vencido ON (dbs_email_vencido.idlan = flan.idlan)
        LEFT JOIN fcfocompl ON (fcfocompl.codcfo = fcfo.codcfo)
        LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
        WHERE flan.statuslan = 'A'
        AND flan.pagrec = 'R'
        AND ftipodoc.classificacao NOT IN ('A', 'P')
        AND flan.codfilial = 1
        AND flan.datavencimento > '{DataInicio}'
        AND POSITION('@', {EmailSQL}) > 0
        AND flan.datavencimento <= (current_date - {QtdDiasDepoisVencer})
        AND flan.codport IS NOT NULL
        AND dbs_email_vencido.idlan IS NULL
        AND flan.codtdo in ('NFe')
        ORDER BY flan.codcfo
    """

    cursor.execute(consulta_sql_cliente_filial1)
    resultados = cursor.fetchall()

    for resultado in resultados:
        codcfo, nomefantasia, email_do_cliente = resultado

        consulta_sql_vencidos_filial1 = f"""
            SELECT ftipodoc.descricao, flan.dataemissao, flan.numerodocumento, flan.datavencimento, flan.valororiginal, flan.idlan
            FROM flan
            LEFT JOIN dbs_email_vencido ON (dbs_email_vencido.idlan = flan.idlan)
            LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
            WHERE flan.statuslan = 'A' AND
                flan.codfilial = 1 AND
                flan.pagrec = 'R' AND
                ftipodoc.classificacao NOT IN ('A', 'P')  AND
                flan.datavencimento > '{DataInicio}' AND
                flan.datavencimento <= (current_date - {QtdDiasDepoisVencer}) AND
                flan.codcfo = '{codcfo}' AND
                flan.codport IS NOT NULL AND
                flan.codtdo in ('NFe') AND
                dbs_email_vencido.idlan IS NULL
            ORDER BY flan.datavencimento
        """

        cursor.execute(consulta_sql_vencidos_filial1)
        resultados_vencidos_filial1 = cursor.fetchall()

        corpo_email_filial1 = f"""
        <html>
        <body style="font-family: 'Arial', sans-serif;">
            <p>Prezado Cliente(a), <strong>{nomefantasia}</strong>.</p>

            <p>Visando cada vez mais o fortalecimento e continuidade da nossa parceria, gostaríamos de informar que, até o presente momento, consta em aberto em nosso sistema a(s) fatura(s) abaixo.</p>

            <table border="1" cellspacing="0" cellpadding="5" width="100%">
            <tr>
                <th style="background-color: #4682B4;">Data Emissão</th>
                <th style="background-color: #4682B4;">Número Documento</th>
                <th style="background-color: #4682B4;">Descrição</th>
                <th style="background-color: #4682B4;">Data Vencimento</th>
                <th style="background-color: #4682B4;">Valor Original</th>
            </tr>
            """

        for resultado_vencido_filial1 in resultados_vencidos_filial1:
            descricao, dataemissao, numerodocumento, datavencimento, valororiginal, idlan = resultado_vencido_filial1

            cursor.execute(
                f"SELECT COUNT(*) FROM dbs_email_vencido WHERE idlan = {idlan}")
            resultado_count = cursor.fetchone()
            count_idlan = resultado_count[0] if resultado_count else 0

            if count_idlan == 0:
                cursor.execute(f"INSERT INTO dbs_email_vencido (idlan, datahora) VALUES ({
                               idlan}, current_timestamp)")
                con.commit()

            # Formatando as datas e valores
            datavencimento = datavencimento.strftime('%d/%m/%Y')
            dataemissao = dataemissao.strftime('%d/%m/%Y')
            valororiginal = f"R$ {valororiginal:.2f}".replace('.', ',')
            # corpo_email += f"\n{dataemissao} - {numerodocumento} - {descricao} - {datavencimento} - {valororiginal}"

            corpo_email_filial1 += f"""
            <tr>
                <td style="text-align: center;">{dataemissao}</td>
                <td style="text-align: center;">{numerodocumento}</td>
                <td style="text-align: center;">{descricao}</td>
                <td style="text-align: center;">{datavencimento}</td>
                <td style="text-align: center;">{valororiginal}</td>
            </tr>
            """

        corpo_email_filial1 += f"""
                </table>

                <p><strong>Informações importantes:</strong></p>

                <p>- O pagamento pode ser efetuado em qualquer banco.</p>

                <p>- Caso não tenha recebido seu boleto ou precise de maiores informações, favor entrar em contato com nosso departamento financeiro pelo numero (34) 3322-8500.</p>
                
                <p>- Este é um e-mail automático enviado para faturas vencidas. Caso o pagamento tenha sido efetuado, favor desconsidera-lo.</p>

                <p>Tenha um ótimo dia,</p>
                
                <br>


            <div style="width: 40%; float: left;">
                <table>
                    <tr>
                        <td>
                            <a href="https://ibb.co/c3Lsb0w"><img src="https://i.ibb.co/sRHX5nC/jenifer222.png" alt="jenifer222" border="0"></a>
                            <p><strong>{CidadeEmpresa} {obter_data_hora_envio()}</strong></p>
                        </td>
                    </tr>
                </table>
            </div>

                   
                </table>
            </div>


            </body>
            </html>
                    """

        destinatarios_boleto = obter_destinatarios([resultado])
        enviar_email_filial1(destinatarios_boleto, corpo_email_filial1)

def buscar_clientes_vencidos_filial3():
    cursor = con.cursor()
    consulta_sql_cliente_filial3 = f"""
        SELECT FIRST {QtdEmailPorVez} DISTINCT fcfo.codcfo, fcfo.nomefantasia, {EmailSQL} AS email
        FROM flan
        INNER JOIN fcfo ON (fcfo.codcfo = flan.codcfo)
        LEFT JOIN dbs_email_vencido ON (dbs_email_vencido.idlan = flan.idlan)
        LEFT JOIN fcfocompl ON (fcfocompl.codcfo = fcfo.codcfo)
        LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
        WHERE flan.statuslan = 'A'
        AND flan.pagrec = 'R'
        AND ftipodoc.classificacao NOT IN ('A', 'P')
        AND flan.codfilial = 3
        AND flan.datavencimento > '{DataInicio}'
        AND POSITION('@', {EmailSQL}) > 0
        AND flan.datavencimento <= (current_date - {QtdDiasDepoisVencer})
        AND flan.codport IS NOT NULL
        AND dbs_email_vencido.idlan IS NULL
        AND flan.codtdo in ('NPe')
        ORDER BY flan.codcfo
    """

    cursor.execute(consulta_sql_cliente_filial3)
    resultados = cursor.fetchall()

    for resultado in resultados:
        codcfo, nomefantasia, email_do_cliente = resultado

        consulta_sql_vencidos_filial3 = f"""
            SELECT ftipodoc.descricao, flan.dataemissao, flan.numerodocumento, flan.datavencimento, flan.valororiginal, flan.idlan
            FROM flan
            LEFT JOIN dbs_email_vencido ON (dbs_email_vencido.idlan = flan.idlan)
            LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
            WHERE flan.statuslan = 'A' AND
                flan.codfilial = 3 AND
                flan.pagrec = 'R' AND
                ftipodoc.classificacao NOT IN ('A', 'P')  AND
                flan.datavencimento > '{DataInicio}' AND
                flan.datavencimento <= (current_date - {QtdDiasDepoisVencer}) AND
                flan.codcfo = '{codcfo}' AND
                flan.codport IS NOT NULL AND
                flan.codtdo in ('NPe') AND
                dbs_email_vencido.idlan IS NULL
            ORDER BY flan.datavencimento
        """

        cursor.execute(consulta_sql_vencidos_filial3)
        resultados_vencidos_filial3 = cursor.fetchall()

        corpo_email_filial3 = f"""
        <html>
        <body style="font-family: 'Arial', sans-serif;">
            <p>Prezado Cliente(a), <strong>{nomefantasia}</strong>.</p>

            <p>Visando cada vez mais o fortalecimento e continuidade da nossa parceria, gostaríamos de informar que, até o presente momento, consta em aberto em nosso sistema a(s) fatura(s) abaixo. </p>

            <table border="1" cellspacing="0" cellpadding="5" width="100%">
            <tr>
                <th style="background-color: #4682B4;">Data Emissão</th>
                <th style="background-color: #4682B4;">Número Documento</th>
                <th style="background-color: #4682B4;">Descrição</th>
                <th style="background-color: #4682B4;">Data Vencimento</th>
                <th style="background-color: #4682B4;">Valor Original</th>
            </tr>
            """

        for resultado_vencido_filial3 in resultados_vencidos_filial3:
            descricao, dataemissao, numerodocumento, datavencimento, valororiginal, idlan = resultado_vencido_filial3

            cursor.execute(
                f"SELECT COUNT(*) FROM dbs_email_vencido WHERE idlan = {idlan}")
            resultado_count = cursor.fetchone()
            count_idlan = resultado_count[0] if resultado_count else 0

            if count_idlan == 0:
                cursor.execute(f"INSERT INTO dbs_email_vencido (idlan, datahora) VALUES ({
                               idlan}, current_timestamp)")
                con.commit()

            # Formatando as datas e valores
            datavencimento = datavencimento.strftime('%d/%m/%Y')
            dataemissao = dataemissao.strftime('%d/%m/%Y')
            valororiginal = f"R$ {valororiginal:.2f}".replace('.', ',')
            # corpo_email += f"\n{dataemissao} - {numerodocumento} - {descricao} - {datavencimento} - {valororiginal}"

            corpo_email_filial3 += f"""
            <tr>
                <td style="text-align: center;">{dataemissao}</td>
                <td style="text-align: center;">{numerodocumento}</td>
                <td style="text-align: center;">{descricao}</td>
                <td style="text-align: center;">{datavencimento}</td>
                <td style="text-align: center;">{valororiginal}</td>
            </tr>
            """

        corpo_email_filial3 += f"""
                </table>

                <p><strong>Informações importantes:</strong></p>

                <p>- O pagamento pode ser efetuado em qualquer banco.</p>

                <p>- Caso não tenha recebido seu boleto ou precise de maiores informações, favor entrar em contato com nosso departamento financeiro pelo numero (34) 3322-8500.</p>
                
                <p>- É importante reforçar que em casos de atraso superior a 10(DEZ) dias corridos, o sistema bloqueia de forma automática.</p>
                
                <p>- Este é um e-mail automático enviado para faturas vencidas. Caso o pagamento tenha sido efetuado, favor desconsidera-lo.</p>
                
                <p>Tenha um ótimo dia,</p>

                
                <br>


            <div style="width: 40%; float: left;">
                <table>
                    <tr>
                        <td>
                             <a href="https://ibb.co/c3Lsb0w"><img src="https://i.ibb.co/sRHX5nC/jenifer222.png" alt="jenifer222" border="0"></a>
                            <p><strong>{CidadeEmpresa} {obter_data_hora_envio()}</strong></p>
                        </td>
                    </tr>
                </table>
            </div>


            </body>
            </html>
                    """

        destinatarios_boleto = obter_destinatarios([resultado])
        enviar_email_filial3(destinatarios_boleto, corpo_email_filial3)


def buscar_clientes_antes_vencimento_filial1():
    cursor = con.cursor()
    consulta_sql_cliente_antes_vencimento_filial1 = f"""
        SELECT FIRST {QtdEmailPorVezAntesVencer} DISTINCT fcfo.codcfo, fcfo.nomefantasia, {EmailSQL} AS email
        FROM flan
        INNER JOIN fcfo ON (fcfo.codcfo = flan.codcfo)
        LEFT JOIN aa_lan_email_avencer ON (aa_lan_email_avencer.idlan = flan.idlan)
        LEFT JOIN fcfocompl ON (fcfocompl.codcfo = fcfo.codcfo)
        LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
        WHERE flan.statuslan = 'A'
        AND flan.pagrec = 'R'
        AND ftipodoc.classificacao NOT IN ('A', 'P')
        AND flan.codfilial = 1
        AND flan.datavencimento > '{DataInicio}'
        AND POSITION('@', {EmailSQL}) > 0
        AND flan.datavencimento = (current_date + {QtdDiasAntesVencer})
        AND flan.codport IS NOT NULL
        AND aa_lan_email_avencer.idlan IS NULL
        AND flan.codtdo in ('NFe')
        ORDER BY flan.codcfo
    """

    cursor.execute(consulta_sql_cliente_antes_vencimento_filial1)
    resultados = cursor.fetchall()

    for resultado in resultados:
        codcfo, nomefantasia, email_do_cliente = resultado

        consulta_sql_cliente_antes_vencimento_filial1 = f"""
            SELECT ftipodoc.descricao, flan.dataemissao, flan.numerodocumento, flan.datavencimento, flan.valororiginal, flan.idlan
            FROM flan
            LEFT JOIN aa_lan_email_avencer ON (aa_lan_email_avencer.idlan = flan.idlan)
            LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
            WHERE flan.statuslan = 'A' AND
                flan.codfilial = 1 AND
                flan.pagrec = 'R' AND
                ftipodoc.classificacao NOT IN ('A', 'P') AND
                flan.datavencimento > '{DataInicio}' AND
                flan.datavencimento = (current_date + {QtdDiasAntesVencer}) AND
                flan.codcfo = '{codcfo}' AND
                flan.codport IS NOT NULL AND
                aa_lan_email_avencer.idlan IS NULL AND
                flan.codtdo in ('NFe')
            ORDER BY flan.datavencimento
        """

        cursor.execute(consulta_sql_cliente_antes_vencimento_filial1)
        resultados_antes_vencimento_filial1 = cursor.fetchall()

        corpo_email_antes_vencimento_filial1 = f"""
        <html>
        <body style="font-family: 'Arial', sans-serif;">
            <p>Prezado Cliente(a), <strong>{nomefantasia}</strong>.</p>

            <p>Informamos que a(s) fatura(s) abaixo está(ão) próxima(s) da data de vencimento. </p>

            <table border="1" cellspacing="0" cellpadding="5" width="100%">
            <tr>
                <th style="background-color: #4682B4;">Data Emissão</th>
                <th style="background-color: #4682B4;">Número Documento</th>
                <th style="background-color: #4682B4;">Descrição</th>
                <th style="background-color: #4682B4;">Data Vencimento</th>
                <th style="background-color: #4682B4;">Valor Original</th>
            </tr>
            """
        for resultado_antes_vencimento_filial1 in resultados_antes_vencimento_filial1:
            descricao, dataemissao, numerodocumento, datavencimento, valororiginal, idlan = resultado_antes_vencimento_filial1

            cursor.execute(
                f"SELECT COUNT(*) FROM aa_lan_email_avencer WHERE idlan = {idlan}")
            resultado_count = cursor.fetchone()
            count_idlan = resultado_count[0] if resultado_count else 0

            if count_idlan == 0:
                cursor.execute(f"INSERT INTO aa_lan_email_avencer (idlan, datahora) VALUES ({idlan}, current_timestamp)")
                con.commit()

            # Formatando as datas e valores
            datavencimento = datavencimento.strftime('%d/%m/%Y')
            dataemissao = dataemissao.strftime('%d/%m/%Y')
            valororiginal = f"R$ {valororiginal:.2f}".replace('.', ',')
            # corpo_email += f"\n{dataemissao} - {numerodocumento} - {descricao} - {datavencimento} - {valororiginal}"

            corpo_email_antes_vencimento_filial1 += f"""
            <tr>
                <td style="text-align: center;">{dataemissao}</td>
                <td style="text-align: center;">{numerodocumento}</td>
                <td style="text-align: center;">{descricao}</td>
                <td style="text-align: center;">{datavencimento}</td>
                <td style="text-align: center;">{valororiginal}</td>
            </tr>
            """

        corpo_email_antes_vencimento_filial1 += f"""
            </table>

            <p><strong>Informações importantes:</strong></p>

            <p>- O pagamento pode ser efetuado em qualquer banco até a data do vencimento.</p>

            <p>- Caso não tenha recebido seu boleto ou precise de maiores informações, favor entrar em contato com nosso departamento financeiro pelo numero (34) 3322-8500</p>
            
    
            <p>Tenha um ótimo dia.</p>
            <br>


            <div style="width: 40%; float: left;">
                <table>
                    <tr>
                        <td>
                            <a href="https://ibb.co/c3Lsb0w"><img src="https://i.ibb.co/sRHX5nC/jenifer222.png" alt="jenifer222" border="0"></a>
                            <p><strong>{CidadeEmpresa} {obter_data_hora_envio()}</strong></p>
                        </td>
                    </tr>
                </table>
            </div>

    
            </body>
            </html>
        """

        destinatarios_boleto = obter_destinatarios([resultado])
        enviar_email_antes_vencimento_filial1(destinatarios_boleto, corpo_email_antes_vencimento_filial1)

def buscar_clientes_antes_vencimento_filial3():
    cursor = con.cursor()
    consulta_sql_cliente_antes_vencimento_filial3 = f"""
        SELECT FIRST {QtdEmailPorVezAntesVencer} DISTINCT fcfo.codcfo, fcfo.nomefantasia, {EmailSQL} AS email
        FROM flan
        INNER JOIN fcfo ON (fcfo.codcfo = flan.codcfo)
        LEFT JOIN aa_lan_email_avencer ON (aa_lan_email_avencer.idlan = flan.idlan)
        LEFT JOIN fcfocompl ON (fcfocompl.codcfo = fcfo.codcfo)
        LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
        WHERE flan.statuslan = 'A'
        AND flan.pagrec = 'R'
        AND ftipodoc.classificacao NOT IN ('A', 'P')
        AND flan.codfilial = 3
        AND flan.datavencimento > '{DataInicio}'
        AND POSITION('@', {EmailSQL}) > 0
        AND flan.datavencimento = (current_date + {QtdDiasAntesVencer})
        AND flan.codport IS NOT NULL
        AND aa_lan_email_avencer.idlan IS NULL
        AND flan.codtdo in ('NPe')
        ORDER BY flan.codcfo
    """

    cursor.execute(consulta_sql_cliente_antes_vencimento_filial3)
    resultados = cursor.fetchall()

    for resultado in resultados:
        codcfo, nomefantasia, email_do_cliente = resultado

        consulta_sql_cliente_antes_vencimento_filial3 = f"""
            SELECT ftipodoc.descricao, flan.dataemissao, flan.numerodocumento, flan.datavencimento, flan.valororiginal, flan.idlan
            FROM flan
            LEFT JOIN aa_lan_email_avencer ON (aa_lan_email_avencer.idlan = flan.idlan)
            LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
            WHERE flan.statuslan = 'A' AND
                flan.codfilial = 3 AND
                flan.pagrec = 'R' AND
                ftipodoc.classificacao NOT IN ('A', 'P') AND
                flan.datavencimento > '{DataInicio}' AND
                flan.datavencimento = (current_date + {QtdDiasAntesVencer}) AND
                flan.codcfo = '{codcfo}' AND
                flan.codport IS NOT NULL AND
                aa_lan_email_avencer.idlan IS NULL AND
                flan.codtdo in ('NPe')
            ORDER BY flan.datavencimento
        """

        cursor.execute(consulta_sql_cliente_antes_vencimento_filial3)
        resultados_antes_vencimento_filial3 = cursor.fetchall()

        corpo_email_antes_vencimento_filial3 = f"""
        <html>
        <body style="font-family: 'Arial', sans-serif;">
            <p>Prezado Cliente(a), <strong>{nomefantasia}</strong>.</p>

            <p>Informamos que a(s) fatura(s) abaixo está(ão) próxima(s) da data de vencimento. </p>

            <table border="1" cellspacing="0" cellpadding="5" width="100%">
            <tr>
                <th style="background-color: #4682B4;">Data Emissão</th>
                <th style="background-color: #4682B4;">Número Documento</th>
                <th style="background-color: #4682B4;">Descrição</th>
                <th style="background-color: #4682B4;">Data Vencimento</th>
                <th style="background-color: #4682B4;">Valor Original</th>
            </tr>
            """
        for resultado_antes_vencimento_filial3 in resultados_antes_vencimento_filial3:
            descricao, dataemissao, numerodocumento, datavencimento, valororiginal, idlan = resultado_antes_vencimento_filial3

            cursor.execute(
                f"SELECT COUNT(*) FROM aa_lan_email_avencer WHERE idlan = {idlan}")
            resultado_count = cursor.fetchone()
            count_idlan = resultado_count[0] if resultado_count else 0

            if count_idlan == 0:
                cursor.execute(f"INSERT INTO aa_lan_email_avencer (idlan, datahora) VALUES ({idlan}, current_timestamp)")
                con.commit()

            # Formatando as datas e valores
            datavencimento = datavencimento.strftime('%d/%m/%Y')
            dataemissao = dataemissao.strftime('%d/%m/%Y')
            valororiginal = f"R$ {valororiginal:.2f}".replace('.', ',')
            # corpo_email += f"\n{dataemissao} - {numerodocumento} - {descricao} - {datavencimento} - {valororiginal}"

            corpo_email_antes_vencimento_filial3 += f"""
            <tr>
                <td style="text-align: center;">{dataemissao}</td>
                <td style="text-align: center;">{numerodocumento}</td>
                <td style="text-align: center;">{descricao}</td>
                <td style="text-align: center;">{datavencimento}</td>
                <td style="text-align: center;">{valororiginal}</td>
            </tr>
            """

        corpo_email_antes_vencimento_filial3 += f"""
            </table>

            <p><strong>Informações importantes:</strong></p>

    
            <p>- O pagamento pode ser efetuado em qualquer banco até a data do vencimento.</p>
            
            
            <p>- Caso não tenha recebido seu boleto ou precise de maiores informações, favor entrar em contato com nosso departamento financeiro pelo numero (34) 3322-8500.</p>
                
            <p>Tenha um ótimo dia.</p>
            <br>


            <div style="width: 40%; float: left;">
                <table>
                    <tr>
                        <td>
                             <a href="https://ibb.co/c3Lsb0w"><img src="https://i.ibb.co/sRHX5nC/jenifer222.png" alt="jenifer222" border="0"></a>
                             <p><strong>{CidadeEmpresa} {obter_data_hora_envio()}</strong></p>
                        </td>
                    </tr>
                </table>
            </div>



            </body>
            </html>
        """

        destinatarios_boleto = obter_destinatarios([resultado])
        enviar_email_antes_vencimento_filial3(destinatarios_boleto, corpo_email_antes_vencimento_filial3)



def buscar_clientes_dia_vencimento_filial1():
    cursor = con.cursor()
    consulta_sql_cliente_dia_vencimento_filial1 = f"""
        SELECT FIRST {QtdEmailPorVezDiaVencimento} DISTINCT fcfo.codcfo, fcfo.nomefantasia, {EmailSQL} AS email
        FROM flan
        INNER JOIN fcfo ON (fcfo.codcfo = flan.codcfo)
        LEFT JOIN AA_LAN_EMAIL_DIA_VENCIMENTO ON (AA_LAN_EMAIL_DIA_VENCIMENTO.idlan = flan.idlan)
        LEFT JOIN fcfocompl ON (fcfocompl.codcfo = fcfo.codcfo)
        LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
        WHERE flan.statuslan = 'A'
        AND flan.pagrec = 'R'
        AND ftipodoc.classificacao NOT IN ('A', 'P')
        AND flan.codfilial = 1
        AND flan.datavencimento > '{DataInicio}'
        AND POSITION('@', {EmailSQL}) > 0
        AND flan.datavencimento = current_date
        AND flan.codport IS NOT NULL
        AND AA_LAN_EMAIL_DIA_VENCIMENTO.idlan IS NULL
        AND flan.codtdo in ('NFe')
        ORDER BY flan.codcfo
    """

    cursor.execute(consulta_sql_cliente_dia_vencimento_filial1)
    resultados = cursor.fetchall()

    for resultado in resultados:
        codcfo, nomefantasia, email_do_cliente = resultado

        consulta_sql_cliente_dia_vencimento_filial1 = f"""
            SELECT ftipodoc.descricao, flan.dataemissao, flan.numerodocumento, flan.datavencimento, flan.valororiginal, flan.idlan
            FROM flan
            LEFT JOIN AA_LAN_EMAIL_DIA_VENCIMENTO ON (AA_LAN_EMAIL_DIA_VENCIMENTO.idlan = flan.idlan)
            LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
            WHERE flan.statuslan = 'A' AND
                flan.codfilial = 1 AND
                flan.pagrec = 'R' AND
                ftipodoc.classificacao NOT IN ('A', 'P')  AND
                flan.datavencimento > '{DataInicio}' AND
                flan.datavencimento = current_date AND
                flan.codcfo = '{codcfo}' AND
                flan.codport IS NOT NULL AND
                AA_LAN_EMAIL_DIA_VENCIMENTO.idlan IS NULL AND
                flan.codtdo in ('NFe')
            ORDER BY flan.datavencimento
        """

        cursor.execute(consulta_sql_cliente_dia_vencimento_filial1)
        resultados_dia_vencimento_filial1 = cursor.fetchall()

        corpo_email_dia_vencimento_filial1 = f"""
        <html>
        <body style="font-family: 'Arial', sans-serif;">
            <p>Prezado Cliente(a), <strong>{nomefantasia}</strong>.</p>
            
            <p>Informamos que a(s) fatura(s) abaixo esta(ão) vencendo na data de hoje. </p>

            <table border="1" cellspacing="0" cellpadding="5" width="100%">
            <tr>
                <th style="background-color: #4682B4;">Data Emissão</th>
                <th style="background-color: #4682B4;">Número Documento</th>
                <th style="background-color: #4682B4;">Descrição</th>
                <th style="background-color: #4682B4;">Data Vencimento</th>
                <th style="background-color: #4682B4;">Valor Original</th>
            </tr>
            """

        for resultado_dia_vencimento_filial1 in resultados_dia_vencimento_filial1:
            descricao, dataemissao, numerodocumento, datavencimento, valororiginal, idlan = resultado_dia_vencimento_filial1

            cursor.execute(
                f"SELECT COUNT(*) FROM AA_LAN_EMAIL_DIA_VENCIMENTO WHERE idlan = {idlan}")
            resultado_count = cursor.fetchone()
            count_idlan = resultado_count[0] if resultado_count else 0

            if count_idlan == 0:
                cursor.execute(f"INSERT INTO AA_LAN_EMAIL_DIA_VENCIMENTO (idlan, datahora) VALUES ({idlan}, current_timestamp)")
                con.commit()

            # Formatando as datas e valores
            datavencimento = datavencimento.strftime('%d/%m/%Y')
            dataemissao = dataemissao.strftime('%d/%m/%Y')
            valororiginal = f"R$ {valororiginal:.2f}".replace('.', ',')
            # corpo_email += f"\n{dataemissao} - {numerodocumento} - {descricao} - {datavencimento} - {valororiginal}"

            corpo_email_dia_vencimento_filial1 += f"""
            <tr>
                <td style="text-align: center;">{dataemissao}</td>
                <td style="text-align: center;">{numerodocumento}</td>
                <td style="text-align: center;">{descricao}</td>
                <td style="text-align: center;">{datavencimento}</td>
                <td style="text-align: center;">{valororiginal}</td>
            </tr>
            """

        corpo_email_dia_vencimento_filial1 += f"""
            </table>

            <p><strong>Informações importantes:</strong></p>

            <p>- O pagamento pode ser efetuado em qualquer banco até a data do vencimento</p>

            <p>- Caso não tenha recebido seu boleto ou precise de maiores informações, favor entrar em contato com nosso departamento financeiro pelo numero (34) 3322-8500.</p>
            
            <p>- Este é um e-mail automático enviado para faturas com a data de vencimento para hoje. Caso o pagamento tenha sido efetuado, favor desconsidera-lo</p>

            <p>Tenha um ótimo dia.</p>
            <br>


            <div style="width: 40%; float: left;">
                <table>
                    <tr>
                        <td>
                            <a href="https://ibb.co/c3Lsb0w"><img src="https://i.ibb.co/sRHX5nC/jenifer222.png" alt="jenifer222" border="0" ></a>
                            <p><strong>{CidadeEmpresa} {obter_data_hora_envio()}</strong></p>
                        </td>
                    </tr>
                </table>
            </div>


            </body>
            </html>
                    """

        destinatarios_boleto = obter_destinatarios([resultado])
        enviar_email_dia_vencimento_filial1(
            destinatarios_boleto, corpo_email_dia_vencimento_filial1)



def buscar_clientes_dia_vencimento_filial3():
    cursor = con.cursor()
    consulta_sql_cliente_dia_vencimento_filial3 = f"""
        SELECT FIRST {QtdEmailPorVezDiaVencimento} DISTINCT fcfo.codcfo, fcfo.nomefantasia, {EmailSQL} AS email
        FROM flan
        INNER JOIN fcfo ON (fcfo.codcfo = flan.codcfo)
        LEFT JOIN AA_LAN_EMAIL_DIA_VENCIMENTO ON (AA_LAN_EMAIL_DIA_VENCIMENTO.idlan = flan.idlan)
        LEFT JOIN fcfocompl ON (fcfocompl.codcfo = fcfo.codcfo)
        LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
        WHERE flan.statuslan = 'A'
        AND flan.pagrec = 'R'
        AND ftipodoc.classificacao NOT IN ('A', 'P')
        AND flan.codfilial = 3
        AND flan.datavencimento > '{DataInicio}'
        AND POSITION('@', {EmailSQL}) > 0
        AND flan.datavencimento = current_date
        AND flan.codport IS NOT NULL
        AND AA_LAN_EMAIL_DIA_VENCIMENTO.idlan IS NULL
        AND flan.codtdo in ('NPe')
        ORDER BY flan.codcfo
    """

    cursor.execute(consulta_sql_cliente_dia_vencimento_filial3)
    resultados = cursor.fetchall()

    for resultado in resultados:
        codcfo, nomefantasia, email_do_cliente = resultado

        consulta_sql_cliente_dia_vencimento_filial3 = f"""
            SELECT ftipodoc.descricao, flan.dataemissao, flan.numerodocumento, flan.datavencimento, flan.valororiginal, flan.idlan
            FROM flan
            LEFT JOIN AA_LAN_EMAIL_DIA_VENCIMENTO ON (AA_LAN_EMAIL_DIA_VENCIMENTO.idlan = flan.idlan)
            LEFT JOIN ftipodoc ON (ftipodoc.codtipodoc = flan.codtdo)
            WHERE flan.statuslan = 'A' AND
                flan.codfilial = 3 AND
                flan.pagrec = 'R' AND
                ftipodoc.classificacao NOT IN ('A', 'P')  AND
                flan.datavencimento > '{DataInicio}' AND
                flan.datavencimento = current_date AND
                flan.codcfo = '{codcfo}' AND
                flan.codport IS NOT NULL AND
                AA_LAN_EMAIL_DIA_VENCIMENTO.idlan IS NULL AND
                flan.codtdo in ('NPe')
            ORDER BY flan.datavencimento
        """

        cursor.execute(consulta_sql_cliente_dia_vencimento_filial3)
        resultados_dia_vencimento_filial3 = cursor.fetchall()

        corpo_email_dia_vencimento_filial3 = f"""
        <html>
        <body style="font-family: 'Arial', sans-serif;">
            <p>Prezado Cliente(a), <strong>{nomefantasia}</strong>.</p>
            
            <p>Informamos que a(s) fatura(s) abaixo esta(ão) vencendo na data de hoje. </p>

            <table border="1" cellspacing="0" cellpadding="5" width="100%">
            <tr>
                <th style="background-color: #4682B4;">Data Emissão</th>
                <th style="background-color: #4682B4;">Número Documento</th>
                <th style="background-color: #4682B4;">Descrição</th>
                <th style="background-color: #4682B4;">Data Vencimento</th>
                <th style="background-color: #4682B4;">Valor Original</th>
            </tr>
            """

        for resultado_dia_vencimento_filial3 in resultados_dia_vencimento_filial3:
            descricao, dataemissao, numerodocumento, datavencimento, valororiginal, idlan = resultado_dia_vencimento_filial3

            cursor.execute(
                f"SELECT COUNT(*) FROM AA_LAN_EMAIL_DIA_VENCIMENTO WHERE idlan = {idlan}")
            resultado_count = cursor.fetchone()
            count_idlan = resultado_count[0] if resultado_count else 0

            if count_idlan == 0:
                cursor.execute(f"INSERT INTO AA_LAN_EMAIL_DIA_VENCIMENTO (idlan, datahora) VALUES ({idlan}, current_timestamp)")
                con.commit()

            # Formatando as datas e valores
            datavencimento = datavencimento.strftime('%d/%m/%Y')
            dataemissao = dataemissao.strftime('%d/%m/%Y')
            valororiginal = f"R$ {valororiginal:.2f}".replace('.', ',')
            # corpo_email += f"\n{dataemissao} - {numerodocumento} - {descricao} - {datavencimento} - {valororiginal}"

            corpo_email_dia_vencimento_filial3 += f"""
            <tr>
                <td style="text-align: center;">{dataemissao}</td>
                <td style="text-align: center;">{numerodocumento}</td>
                <td style="text-align: center;">{descricao}</td>
                <td style="text-align: center;">{datavencimento}</td>
                <td style="text-align: center;">{valororiginal}</td>
            </tr>
            """

        corpo_email_dia_vencimento_filial3 += f"""
            </table>

            <p><strong>Informações importantes:</strong></p>

            <p>- O pagamento pode ser efetuado em qualquer banco até a data do vencimento</p>
            
            <p>- Caso não tenha recebido seu boleto ou precise de maiores informações, favor entrar em contato com nosso departamento financeiro pelo numero (34) 3322-8500.</p>
            
            <p>- Este é um e-mail automático enviado para faturas com a data de vencimento para hoje. Caso o pagamento tenha sido efetuado, favor desconsidera-lo</p>
                
            <p>Tenha um ótimo dia.</p>
            <br>


            <div style="width: 40%; float: left;">
                <table>
                    <tr>
                        <td>
                            <a href="https://ibb.co/c3Lsb0w"><img src="https://i.ibb.co/sRHX5nC/jenifer222.png" alt="jenifer222" border="0"></a>
                            <p style="margin-left: 10px;"><strong>{CidadeEmpresa} {obter_data_hora_envio()}</strong></p>
                        </td>
                    </tr>
                </table>
            </div>


            </body>
            </html>
                    """

        destinatarios_boleto = obter_destinatarios([resultado])
        enviar_email_dia_vencimento_filial3(
            destinatarios_boleto, corpo_email_dia_vencimento_filial3)
    con.close()



    # Criar tabela se não existir
criar_tabela_dbs_email_vencido()
criar_tabela_dbs_email_antes_vencimento()
criar_tabela_dbs_email_dia_vencimento()

# Buscar clientes vencidos e enviar e-mails
buscar_clientes_vencidos_filial1()
buscar_clientes_antes_vencimento_filial1()
buscar_clientes_dia_vencimento_filial1()
buscar_clientes_vencidos_filial3()
buscar_clientes_antes_vencimento_filial3()
buscar_clientes_dia_vencimento_filial3()