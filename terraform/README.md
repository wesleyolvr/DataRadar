# Terraform - DataRadar Infrastructure

Infraestrutura como código para o projeto DataRadar.

## 📦 Recursos Provisionados

- **S3 Bucket** (`devradar-raw`): storage Bronze com lifecycle policy
- **Lambda Function**: trigger Databricks quando novos arquivos chegam no S3
- **IAM Roles/Policies**: permissões Lambda (S3 read, SSM read, CloudWatch logs)
- **S3 Event Notifications**: dispara Lambda automaticamente
- **SSM Parameter Store**: secrets centralizados (Databricks token, Groq key)
- **CloudWatch Log Group**: logs Lambda

## 🚀 Quick Start

### 1. Pré-requisitos

```bash
# Terraform instalado
terraform --version  # >= 1.0

# AWS CLI configurado
aws configure
aws sts get-caller-identity  # Verificar credenciais
```

### 2. Configurar Variáveis

```bash
# Copiar template
cp terraform.tfvars.example terraform.tfvars

# Editar com seus valores reais
# IMPORTANTE: terraform.tfvars está no .gitignore!
nano terraform.tfvars
```

### 3. Criar ZIP da Lambda

```bash
# Na raiz do projeto
cd lambda
zip handler.zip handler.py
cd ../terraform
```

### 4. Deploy

```bash
# Inicializar Terraform
terraform init

# Ver plano de execução
terraform plan

# Aplicar mudanças
terraform apply
```

## 📁 Estrutura

```
terraform/
├── main.tf              # Provider e configuração
├── variables.tf         # Variáveis de entrada
├── s3.tf               # S3 bucket + lifecycle + events
├── lambda.tf           # Lambda function + permissions
├── iam.tf              # IAM roles e policies
├── ssm.tf              # Parameter Store (secrets)
├── outputs.tf          # Outputs (ARNs, nomes, etc)
├── terraform.tfvars.example  # Template vars
└── README.md           # Este arquivo
```

## 🌍 Ambientes (dev/prod)

Para criar ambiente dev:

```bash
terraform apply -var="environment=dev"
```

Namespace: `s3://devradar-raw/dev/reddit/...`

## 🔒 Secrets Management

Secrets armazenados no AWS SSM Parameter Store:

```
/devradar/prod/databricks_host
/devradar/prod/databricks_token
/devradar/prod/databricks_job_id
/devradar/prod/groq_api_key
```

Ler via CLI:
```bash
aws ssm get-parameter --name "/devradar/prod/databricks_token" --with-decryption
```

## 🗑️ Destruir Recursos

```bash
terraform destroy
```

⚠️ **Atenção:** Isso deleta S3 bucket e todos os dados!

## 📊 Custo Estimado

- **S3**: ~$0.02/GB/mês (dentro free tier: 5GB)
- **Lambda**: Primeiras 1M requests grátis
- **SSM**: Gratuito (até 10K parameters)
- **CloudWatch Logs**: 5GB grátis

**Total:** $0/mês (dentro free tier)

## 🔄 CI/CD Integration

Este código é executado automaticamente pelo GitHub Actions:

```yaml
# .github/workflows/deploy-infra.yml
- name: Terraform Apply
  run: |
    cd terraform
    terraform init
    terraform apply -auto-approve
```

Secrets vêm do GitHub Actions via variáveis de ambiente:
```bash
export TF_VAR_databricks_token=${{ secrets.DATABRICKS_TOKEN }}
```
