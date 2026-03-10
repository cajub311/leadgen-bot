#!/bin/bash
# Temporary script to set repository secrets for Nebula webhook integration
# Run this from your Codespace or any environment with gh CLI authenticated
# Usage: bash setup-secrets.sh
# After running, delete this file: git rm setup-secrets.sh && git commit -m 'chore: remove temp secrets setup script' && git push

set -e

echo "Setting NEBULA_WEBHOOK_URL secret..."
echo 'https://api.nebula.gg/webhooks/triggers/trig_069b025b8dbb76e18000193a46e2a0fc/webhook' | gh secret set NEBULA_WEBHOOK_URL --repo cajub311/leadgen-bot

echo "Setting NEBULA_WEBHOOK_SECRET secret..."
echo 'Q6wfY71vIuuvpwQW6VNu6pGFYC7Tal5eEK4Vp1mjCCA' | gh secret set NEBULA_WEBHOOK_SECRET --repo cajub311/leadgen-bot

echo ""
echo "Done! Both secrets have been set."
echo "You can verify at: https://github.com/cajub311/leadgen-bot/settings/secrets/actions"
echo ""
echo "Now clean up this script:"
echo "  git rm setup-secrets.sh && git commit -m 'chore: remove temp secrets setup script' && git push"
