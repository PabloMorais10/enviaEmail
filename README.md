API de Consulta de Pendências Financeiras e Envio Automático de E-mails
Descrição
Esta API realiza consultas de pendências financeiras de clientes em um banco de dados Firebird e envia automaticamente e-mails de notificação. A API é configurável para operar em dois modos: Homologação (envia e-mails apenas para um endereço específico para testes) e Produção (envia e-mails para os clientes reais). Os e-mails enviados podem incluir lembretes de vencimento, notificações de pendências vencidas e outros avisos financeiros.

Funcionalidades
Consulta de Pendências Financeiras: A API consulta um banco de dados Firebird para obter informações sobre títulos em aberto de clientes, de acordo com os critérios configurados.

Envio Automático de E-mails: Dependendo do modo configurado, a API envia e-mails de notificação para os clientes, com a opção de enviar cópias para o e-mail da empresa.

Configuração Flexível: Permite configurar parâmetros como o período de consulta, o número de e-mails enviados por vez, a aparência dos e-mails, e muito mais.

Configuração
Antes de executar a API, ajuste as seguintes configurações no código:

Configurações Gerais

MetodoWorkFlow: Define o modo de operação (Producao ou Homologacao).
FiliaisParticipantes: Lista de filiais participantes, separadas por vírgula.
DataInicio: Data inicial para consulta das pendências financeiras.
Parâmetros de E-mail

QtdDiasAntesVencer, QtdDiasDepoisVencer: Configuram os prazos para envio de e-mails antes e depois do vencimento dos títulos.
QtdEmailPorVez, QtdEmailPorVezAntesVencer, QtdEmailPorVezDiaVencimento: Definem o número máximo de e-mails enviados em cada operação.
Parâmetros do Layout do E-mail

CaminhoLogo: URL da logo da empresa que será exibida nos e-mails.
CorEmail: Cor do layout do e-mail.
Outros parâmetros que controlam o conteúdo e a aparência dos e-mails enviados.
Dados de Conexão com o Banco de Dados

database_path: Caminho para o banco de dados Firebird.
user, password: Credenciais de acesso ao banco de dados.
server_ip, port: Endereço e porta do servidor de banco de dados.

Dependências
O projeto utiliza várias bibliotecas e ferramentas para seu funcionamento, como Flask para o backend, Pandas para manipulação de dados e PDFKit para geração de PDFs.

Estrutura do Código
Conexão com o Banco de Dados: Estabelece a conexão com o banco de dados Firebird e realiza as consultas necessárias.
Envio de E-mails: Envia e-mails de acordo com as pendências financeiras encontradas e o modo de operação configurado.
Funções Auxiliares: Inclui funções para criar tabelas auxiliares, formatar datas, e preparar o conteúdo dos e-mails.

## Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para mais detalhes.



