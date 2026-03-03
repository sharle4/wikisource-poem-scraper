# -----------------------------------------------------------
# Download French Wikisource HTML dumps from Wikimedia Enterprise
# -----------------------------------------------------------
# Prerequisites:
#   - A free account at https://enterprise.wikimedia.com/
#   - curl.exe available in PATH (ships with Windows 10+)
#
# Usage:
#   1. Edit the $username and $password variables below.
#   2. Run: .\download_enterprise_dumps.ps1
# -----------------------------------------------------------

# -- Configuration: your Wikimedia Enterprise credentials --
$username = "username"        # your lowercase username
$password = "YourPassword"    # your password

# -- Step 1: Authenticate and retrieve an access token --
$body = @{
    username = $username
    password = $password
} | ConvertTo-Json -Compress

Write-Host "Logging in to Wikimedia Enterprise..."
$response = Invoke-RestMethod -Method Post `
    -Uri "https://auth.enterprise.wikimedia.com/v1/login" `
    -ContentType "application/json" `
    -Body $body

$TOKEN = $response.access_token
if (-not $TOKEN) {
    Write-Host "ERROR: Login failed. Check your credentials." -ForegroundColor Red
    exit 1
}
Write-Host "Access token retrieved (length: $($TOKEN.Length))"

# -- Step 2: Download the French Wikisource HTML dump --
$dump_url = "https://api.enterprise.wikimedia.com/v2/snapshots/frwikisource_namespace_0/download"
$output_file = "frwikisource.tar.gz"

Write-Host "Downloading French Wikisource HTML dump (~21 GB)..."
Write-Host "This may take a while depending on your connection."
curl.exe -H "Authorization: Bearer $TOKEN" -L $dump_url --output $output_file

if (-not (Test-Path $output_file)) {
    Write-Host "ERROR: Download failed. File not found." -ForegroundColor Red
    exit 1
}

# -- Step 3: Extract the tar.gz archive --
Write-Host "Extracting dump archive..."
tar -xzf $output_file

Write-Host ""
Write-Host "Done! NDJSON files are now available in the current directory." -ForegroundColor Green
Write-Host "You can pass this directory as --dumps-dir when running offline mode."
