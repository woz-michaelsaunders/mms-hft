# Installing kdb+ from KX Portal

## Prerequisites
1. You need a KX Developer account to access the download portal
2. Visit: https://portal.dl.kx.com/assets/raw/kdb%2B/4.1/2025.12.18/
3. Log in with your KX credentials

## Installation Steps

### 1. Download kdb+ for Linux
Once authenticated, download the Linux version:
- File name will likely be: `linux.zip` or `l64.zip` (for 64-bit Linux)
- Or check the directory listing for the exact filename

### 2. Download the file
You can either:
- Download manually from the browser after logging in
- Or use wget/curl with authentication:

```bash
# Option 1: Download manually and transfer to server
# Then run:
cd ~/q
unzip ~/Downloads/linux.zip

# Option 2: Use wget with authentication (if you have credentials)
wget --user=YOUR_EMAIL --password=YOUR_PASSWORD \
  "https://portal.dl.kx.com/assets/raw/kdb%2B/4.1/2025.12.18/linux.zip" \
  -O ~/q/kdb_linux.zip
```

### 3. Extract kdb+
```bash
mkdir -p ~/q
cd ~/q
unzip kdb_linux.zip
# Or if it's a tar.gz:
# tar -xzf kdb_linux.tar.gz
```

### 4. Set up environment variables
Add to your `~/.bashrc`:
```bash
export QHOME=~/q
export PATH=$PATH:$QHOME/l64  # For 64-bit Linux
```

Then reload:
```bash
source ~/.bashrc
```

### 5. Install license (if you have one)
```bash
cp /path/to/your/k4.lic ~/q/
# Or
cp /path/to/your/kc.lic ~/q/
```

### 6. Test the installation
```bash
cd ~/q/l64
./q
```

You should see the kdb+ prompt: `q)`

Test with:
```q
q) til 6
0 1 2 3 4 5
```

Exit with: `\\\\`

## Alternative: Manual Download
If you have access to the portal, you can:
1. Log in at: https://auth.developer.kx.com
2. Navigate to the download page
3. Download `linux.zip` manually
4. Transfer it to your server using `scp` or similar
5. Follow steps 3-6 above

## Notes
- kdb+ will run in trial mode if no license is provided
- The trial has some limitations (32-bit integers, limited memory, etc.)
- For production use, you'll need a valid license file

