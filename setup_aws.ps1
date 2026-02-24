<# 
.SYNOPSIS
    Setup script for the Data Quality Governance Pipeline.

.DESCRIPTION
    Installs dependencies, configures AWS credentials, and validates
    the S3 bucket connection before running the pipeline.

.EXAMPLE
    .\setup_aws.ps1
    .\setup_aws.ps1 -BucketName "my-data-bucket" -Region "eu-west-1"
#>

param(
    [string]$BucketName,
    [string]$Region = "us-east-1"
)

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Data Quality Governance Pipeline — AWS S3 Setup" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# ---------------------------------------------------------------
# Step 1: Install Python dependencies
# ---------------------------------------------------------------
Write-Host "[1/5] Installing Python dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt
pip install boto3
Write-Host "  Done." -ForegroundColor Green
Write-Host ""

# ---------------------------------------------------------------
# Step 2: Check AWS CLI
# ---------------------------------------------------------------
Write-Host "[2/5] Checking AWS CLI..." -ForegroundColor Yellow
$awsCli = Get-Command aws -ErrorAction SilentlyContinue
if ($awsCli) {
    Write-Host "  AWS CLI found: $($awsCli.Source)" -ForegroundColor Green
} else {
    Write-Host "  AWS CLI not found. You can install it from:" -ForegroundColor Red
    Write-Host "  https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
    Write-Host "  Alternatively, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars."
    Write-Host ""
}

# ---------------------------------------------------------------
# Step 3: Prompt for bucket name if not provided
# ---------------------------------------------------------------
Write-Host "[3/5] Configuring S3 bucket..." -ForegroundColor Yellow
if (-not $BucketName) {
    $BucketName = Read-Host "  Enter your S3 bucket name"
}
if (-not $BucketName) {
    Write-Host "  ERROR: Bucket name is required." -ForegroundColor Red
    exit 1
}
Write-Host "  Bucket: $BucketName" -ForegroundColor Green
Write-Host "  Region: $Region" -ForegroundColor Green
Write-Host ""

# ---------------------------------------------------------------
# Step 4: Set environment variables for this session
# ---------------------------------------------------------------
Write-Host "[4/5] Setting environment variables..." -ForegroundColor Yellow
$env:CLOUD_STORAGE_ENABLED = "true"
$env:CLOUD_PROVIDER = "s3"
$env:CLOUD_BUCKET = $BucketName
$env:CLOUD_REGION = $Region
$env:CLOUD_RAW_PREFIX = "raw/"
$env:CLOUD_CLEANED_PREFIX = "cleaned/"
$env:CLOUD_REPORTS_PREFIX = "reports/"
$env:CLOUD_QUARANTINE_PREFIX = "quarantine/"

Write-Host "  CLOUD_STORAGE_ENABLED = true" -ForegroundColor Green
Write-Host "  CLOUD_BUCKET          = $BucketName" -ForegroundColor Green
Write-Host "  CLOUD_REGION          = $Region" -ForegroundColor Green
Write-Host ""

# ---------------------------------------------------------------
# Step 5: Validate S3 connection
# ---------------------------------------------------------------
Write-Host "[5/5] Validating S3 connection..." -ForegroundColor Yellow

$testScript = @"
import boto3, sys
try:
    s3 = boto3.client('s3', region_name='$Region')
    s3.head_bucket(Bucket='$BucketName')
    print('  SUCCESS: Connected to s3://$BucketName')
    sys.exit(0)
except Exception as e:
    print(f'  FAILED: {e}')
    print('  Check your AWS credentials and bucket permissions.')
    sys.exit(1)
"@

$testScript | python -

if ($LASTEXITCODE -eq 0) {
    Write-Host "" 
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "  Setup complete! You can now run the pipeline:" -ForegroundColor Green
    Write-Host "" 
    Write-Host "    python main.py" -ForegroundColor White
    Write-Host "" 
    Write-Host "  Pipeline outputs will upload to:" -ForegroundColor Green
    Write-Host "    s3://$BucketName/cleaned/   (cleaned + masked CSVs)" -ForegroundColor White
    Write-Host "    s3://$BucketName/reports/   (quality reports)" -ForegroundColor White
    Write-Host "    s3://$BucketName/quarantine/ (failed records)" -ForegroundColor White
    Write-Host "============================================================" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Red
    Write-Host "  S3 connection failed. To fix:" -ForegroundColor Red
    Write-Host "    1. Run: aws configure" -ForegroundColor White
    Write-Host "    2. Or set: `$env:AWS_ACCESS_KEY_ID = '...'" -ForegroundColor White
    Write-Host "             `$env:AWS_SECRET_ACCESS_KEY = '...'" -ForegroundColor White
    Write-Host "    3. Ensure bucket '$BucketName' exists and you have access" -ForegroundColor White
    Write-Host "============================================================" -ForegroundColor Red
}
