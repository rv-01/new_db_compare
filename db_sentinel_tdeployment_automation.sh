#!/bin/bash

# DB-Sentinel Production Deployment Script
# ========================================
# 
# Complete production deployment automation for DB-Sentinel
# Supports multiple environments, health checks, and rollback capability
#
# Usage: ./deploy/deploy_db_sentinel.sh [environment] [version]

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." &> /dev/null && pwd)"
DEPLOYMENT_USER="${DEPLOYMENT_USER:-dbsentinel}"
SERVICE_NAME="db-sentinel"
VERSION="${2:-latest}"
ENVIRONMENT="${1:-production}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m'

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_header() {
    echo -e "${PURPLE}[DEPLOY]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Function to print banner
print_banner() {
    echo -e "${BLUE}"
    cat << 'EOF'
╔══════════════════════════════════════════════════════════════╗
║                     🛡️  DB-SENTINEL                         ║
║              Enterprise Database Comparison Tool             ║
║                   Production Deployment                      ║
╚══════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
}

# Function to validate environment
validate_environment() {
    log_info "Validating deployment environment: $ENVIRONMENT"
    
    case "$ENVIRONMENT" in
        development|dev)
            ENVIRONMENT="development"
            ;;
        staging|stage)
            ENVIRONMENT="staging"
            ;;
        production|prod)
            ENVIRONMENT="production"
            ;;
        *)
            log_error "Invalid environment: $ENVIRONMENT"
            log_error "Valid environments: development, staging, production"
            exit 1
            ;;
    esac
    
    log_success "Environment validated: $ENVIRONMENT"
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking deployment prerequisites..."
    
    # Check if running as correct user
    if [ "$(whoami)" != "$DEPLOYMENT_USER" ] && [ "$(whoami)" != "root" ]; then
        log_error "This script must be run as $DEPLOYMENT_USER or root"
        exit 1
    fi
    
    # Check Python version
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is required but not installed"
        exit 1
    fi
    
    python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    if ! python3 -c 'import sys; exit(0 if sys.version_info >= (3, 8) else 1)'; then
        log_error "Python 3.8+ is required. Found: $python_version"
        exit 1
    fi
    
    # Check pip
    if ! command -v pip3 &> /dev/null; then
        log_error "pip3 is required but not installed"
        exit 1
    fi
    
    # Check disk space
    available_space=$(df /opt --output=avail | tail -n 1)
    required_space=2097152  # 2GB in KB
    
    if [ "$available_space" -lt "$required_space" ]; then
        log_error "Insufficient disk space. Required: 2GB, Available: $(( available_space / 1024 / 1024 ))GB"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Function to setup directories
setup_directories() {
    log_info "Setting up directory structure..."
    
    # Create application directories
    sudo mkdir -p /opt/db-sentinel/{config,logs,output,archive,backup,scripts}
    sudo mkdir -p /var/log/db-sentinel
    sudo mkdir -p /etc/db-sentinel
    
    # Create environment-specific directories
    sudo mkdir -p /opt/db-sentinel/environments/$ENVIRONMENT
    
    # Set ownership
    sudo chown -R $DEPLOYMENT_USER:$DEPLOYMENT_USER /opt/db-sentinel
    sudo chown -R $DEPLOYMENT_USER:$DEPLOYMENT_USER /var/log/db-sentinel
    sudo chown -R $DEPLOYMENT_USER:$DEPLOYMENT_USER /etc/db-sentinel
    
    # Set permissions
    sudo chmod 755 /opt/db-sentinel
    sudo chmod 750 /opt/db-sentinel/config
    sudo chmod 755 /opt/db-sentinel/{logs,output,archive}
    sudo chmod 700 /opt/db-sentinel/backup
    
    log_success "Directory structure created"
}

# Function to backup current installation
backup_current_installation() {
    log_info "Creating backup of current installation..."
    
    backup_dir="/opt/db-sentinel/backup/$(date +%Y%m%d_%H%M%S)"
    sudo mkdir -p "$backup_dir"
    
    # Backup application files
    if [ -f "/opt/db-sentinel/db_sentinel.py" ]; then
        sudo cp -r /opt/db-sentinel/*.py "$backup_dir/" 2>/dev/null || true
        sudo cp -r /opt/db-sentinel/scripts "$backup_dir/" 2>/dev/null || true
    fi
    
    # Backup configuration
    if [ -d "/opt/db-sentinel/config" ]; then
        sudo cp -r /opt/db-sentinel/config "$backup_dir/" 2>/dev/null || true
    fi
    
    # Backup virtual environment info
    if [ -d "/opt/db-sentinel/venv" ]; then
        pip freeze > "$backup_dir/requirements_backup.txt" 2>/dev/null || true
    fi
    
    # Cleanup old backups (keep last 5)
    backup_count=$(find /opt/db-sentinel/backup -maxdepth 1 -type d -name "20*" | wc -l)
    if [ "$backup_count" -gt 5 ]; then
        find /opt/db-sentinel/backup -maxdepth 1 -type d -name "20*" | \
        sort | head -n $((backup_count - 5)) | \
        xargs sudo rm -rf
    fi
    
    log_success "Backup created: $backup_dir"
    echo "BACKUP_DIR=$backup_dir" > /tmp/db-sentinel-backup.env
}

# Function to install Python dependencies
install_dependencies() {
    log_info "Installing Python dependencies..."
    
    cd /opt/db-sentinel
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv venv
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
        pip install -r "$PROJECT_ROOT/requirements.txt"
    else
        # Install basic dependencies if requirements.txt not found
        pip install oracledb pandas pyyaml tqdm pytest
    fi
    
    log_success "Dependencies installed"
}

# Function to deploy application files
deploy_application_files() {
    log_info "Deploying application files..."
    
    # Copy main application files
    sudo cp "$PROJECT_ROOT/db_sentinel.py" /opt/db-sentinel/
    
    # Copy scripts directory
    if [ -d "$PROJECT_ROOT/scripts" ]; then
        sudo cp -r "$PROJECT_ROOT/scripts" /opt/db-sentinel/
    fi
    
    # Copy tests if they exist
    if [ -d "$PROJECT_ROOT/tests" ]; then
        sudo cp -r "$PROJECT_ROOT/tests" /opt/db-sentinel/
    fi
    
    # Copy documentation
    if [ -f "$PROJECT_ROOT/README_DB-Sentinel.md" ]; then
        sudo cp "$PROJECT_ROOT/README_DB-Sentinel.md" /opt/db-sentinel/
    fi
    
    # Make scripts executable
    sudo chmod +x /opt/db-sentinel/scripts/*.py
    sudo chmod +x /opt/db-sentinel/db_sentinel.py
    
    # Set ownership
    sudo chown -R $DEPLOYMENT_USER:$DEPLOYMENT_USER /opt/db-sentinel
    
    log_success "Application files deployed"
}

# Function to deploy configuration
deploy_configuration() {
    log_info "Deploying configuration for environment: $ENVIRONMENT"
    
    # Copy environment-specific configuration if it exists
    env_config_dir="$PROJECT_ROOT/environments/$ENVIRONMENT"
    if [ -d "$env_config_dir" ]; then
        sudo cp -r "$env_config_dir"/* /opt/db-sentinel/environments/$ENVIRONMENT/
    fi
    
    # Copy sample configuration
    if [ -f "$PROJECT_ROOT/config_dbsentinel.yaml" ]; then
        sudo cp "$PROJECT_ROOT/config_dbsentinel.yaml" /opt/db-sentinel/config/config_sample.yaml
    fi
    
    # Create environment-specific config if it doesn't exist
    env_config_file="/opt/db-sentinel/config/config_${ENVIRONMENT}.yaml"
    if [ ! -f "$env_config_file" ]; then
        log_warning "Environment-specific config not found. Creating from sample..."
        sudo cp /opt/db-sentinel/config/config_sample.yaml "$env_config_file"
        
        # Update environment-specific values
        sudo sed -i "s/source_host/${ENVIRONMENT}_source_host/g" "$env_config_file"
        sudo sed -i "s/target_host/${ENVIRONMENT}_target_host/g" "$env_config_file"
        sudo sed -i "s/source_user/${ENVIRONMENT}_source_user/g" "$env_config_file"
        sudo sed -i "s/target_user/${ENVIRONMENT}_target_user/g" "$env_config_file"
    fi
    
    log_success "Configuration deployed"
}

# Function to run tests
run_tests() {
    log_info "Running test suite..."
    
    cd /opt/db-sentinel
    source venv/bin/activate
    
    # Run unit tests if test directory exists
    if [ -d "tests" ]; then
        python -m pytest tests/ -v --tb=short
        if [ $? -eq 0 ]; then
            log_success "All tests passed"
        else
            log_error "Some tests failed"
            if [ "$ENVIRONMENT" == "production" ]; then
                log_error "Cannot deploy to production with failing tests"
                exit 1
            else
                log_warning "Continuing deployment despite test failures (non-production environment)"
            fi
        fi
    else
        log_warning "No tests found, skipping test execution"
    fi
}

# Function to validate configuration
validate_configuration() {
    log_info "Validating configuration..."
    
    cd /opt/db-sentinel
    source venv/bin/activate
    
    config_file="/opt/db-sentinel/config/config_${ENVIRONMENT}.yaml"
    
    if [ -f "$config_file" ]; then
        python scripts/db_sentinel_utils.py validate "$config_file"
        if [ $? -eq 0 ]; then
            log_success "Configuration validation passed"
        else
            log_error "Configuration validation failed"
            exit 1
        fi
    else
        log_warning "Configuration file not found: $config_file"
        log_warning "Skipping configuration validation"
    fi
}

# Function to setup systemd service
setup_systemd_service() {
    log_info "Setting up systemd service..."
    
    # Create systemd service file
    cat << EOF | sudo tee /etc/systemd/system/db-sentinel.service
[Unit]
Description=DB-Sentinel Database Comparison Service
After=network.target

[Service]
Type=oneshot
User=$DEPLOYMENT_USER
Group=$DEPLOYMENT_USER
WorkingDirectory=/opt/db-sentinel
Environment=PATH=/opt/db-sentinel/venv/bin
ExecStart=/opt/db-sentinel/venv/bin/python /opt/db-sentinel/db_sentinel.py /opt/db-sentinel/config/config_${ENVIRONMENT}.yaml
StandardOutput=journal
StandardError=journal
SyslogIdentifier=db-sentinel

[Install]
WantedBy=multi-user.target
EOF
    
    # Create timer for scheduled execution if this is production
    if [ "$ENVIRONMENT" == "production" ]; then
        cat << EOF | sudo tee /etc/systemd/system/db-sentinel.timer
[Unit]
Description=Run DB-Sentinel daily
Requires=db-sentinel.service

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
EOF
        
        # Enable and start timer
        sudo systemctl daemon-reload
        sudo systemctl enable db-sentinel.timer
        sudo systemctl start db-sentinel.timer
        
        log_success "Systemd timer configured for daily execution"
    else
        sudo systemctl daemon-reload
        sudo systemctl enable db-sentinel.service
        log_success "Systemd service configured"
    fi
}

# Function to setup log rotation
setup_log_rotation() {
    log_info "Setting up log rotation..."
    
    cat << EOF | sudo tee /etc/logrotate.d/db-sentinel
/opt/db-sentinel/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $DEPLOYMENT_USER $DEPLOYMENT_USER
    copytruncate
}

/var/log/db-sentinel/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $DEPLOYMENT_USER $DEPLOYMENT_USER
    copytruncate
}
EOF
    
    log_success "Log rotation configured"
}

# Function to perform health check
health_check() {
    log_info "Performing health check..."
    
    cd /opt/db-sentinel
    source venv/bin/activate
    
    # Test Python imports
    python -c "import db_sentinel; print('✅ Main module imports successfully')"
    python -c "import scripts.db_sentinel_utils; print('✅ Utilities module imports successfully')"
    
    # Test configuration loading
    config_file="/opt/db-sentinel/config/config_${ENVIRONMENT}.yaml"
    if [ -f "$config_file" ]; then
        python -c "
import yaml
with open('$config_file', 'r') as f:
    config = yaml.safe_load(f)
print('✅ Configuration loads successfully')
"
    fi
    
    log_success "Health check passed"
}

# Function to send deployment notification
send_notification() {
    local status="$1"
    local message="$2"
    
    # Log to system log
    logger "DB-Sentinel Deployment $status: $message"
    
    # Send email notification if configured
    if command -v mail &> /dev/null && [ -n "${ADMIN_EMAIL:-}" ]; then
        echo "$message" | mail -s "DB-Sentinel Deployment $status" "$ADMIN_EMAIL"
    fi
}

# Function to display deployment summary
show_deployment_summary() {
    log_info "Deployment Summary"
    echo "======================================="
    echo "Environment: $ENVIRONMENT"
    echo "Version: $VERSION"
    echo "Deployment Time: $(date)"
    echo "Installation Path: /opt/db-sentinel"
    echo ""
    echo "Configuration:"
    echo "  Config File: /opt/db-sentinel/config/config_${ENVIRONMENT}.yaml"
    echo "  Log Directory: /opt/db-sentinel/logs"
    echo "  Output Directory: /opt/db-sentinel/output"
    echo ""
    echo "Service Management:"
    echo "  Status: sudo systemctl status db-sentinel"
    echo "  Logs: sudo journalctl -u db-sentinel -f"
    echo "  Manual Run: sudo -u $DEPLOYMENT_USER /opt/db-sentinel/venv/bin/python /opt/db-sentinel/db_sentinel.py /opt/db-sentinel/config/config_${ENVIRONMENT}.yaml"
    echo ""
    echo "Management Commands:"
    echo "  Validate Config: cd /opt/db-sentinel && ./scripts/db_sentinel_utils.py validate config/config_${ENVIRONMENT}.yaml"
    echo "  Discover Tables: cd /opt/db-sentinel && ./scripts/db_sentinel_utils.py discover --help"
    echo "  View Logs: tail -f /opt/db-sentinel/logs/*.log"
    echo "======================================="
}

# Main deployment function
main() {
    print_banner
    
    log_header "Starting DB-Sentinel deployment"
    log_header "Environment: $ENVIRONMENT, Version: $VERSION"
    
    # Deployment steps
    validate_environment
    check_prerequisites
    setup_directories
    backup_current_installation
    install_dependencies
    deploy_application_files
    deploy_configuration
    run_tests
    validate_configuration
    setup_systemd_service
    setup_log_rotation
    health_check
    
    # Success notification
    send_notification "SUCCESS" "DB-Sentinel $VERSION deployed successfully to $ENVIRONMENT"
    
    show_deployment_summary
    log_success "🎉 DB-Sentinel deployment completed successfully!"
}

# Error handling
trap 'send_notification "FAILED" "DB-Sentinel deployment failed at line $LINENO in $ENVIRONMENT"' ERR

# Command line help
case "${1:-}" in
    --help|-h)
        echo "DB-Sentinel Deployment Script"
        echo ""
        echo "Usage: $0 [environment] [version]"
        echo ""
        echo "Environments:"
        echo "  development, dev    - Development environment"
        echo "  staging, stage      - Staging environment"
        echo "  production, prod    - Production environment"
        echo ""
        echo "Examples:"
        echo "  $0 development                    # Deploy latest to development"
        echo "  $0 production v1.2.3             # Deploy version 1.2.3 to production"
        echo "  $0 staging                        # Deploy latest to staging"
        echo ""
        echo "Environment Variables:"
        echo "  DEPLOYMENT_USER     - User to run the service (default: dbsentinel)"
        echo "  ADMIN_EMAIL         - Email for deployment notifications"
        exit 0
        ;;
    --version)
        echo "DB-Sentinel Deployment Script v1.0.0"
        exit 0
        ;;
esac

# Validate arguments
if [ $# -lt 1 ]; then
    log_error "Environment argument is required"
    echo "Usage: $0 [environment] [version]"
    echo "Run '$0 --help' for more information"
    exit 1
fi

# Run main deployment
main "$@"
