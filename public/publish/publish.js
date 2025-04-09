class UpdateMetaData {
    constructor() {
        this.elements = {
            form: document.getElementById('publishForm'),
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            pechaSelect: document.getElementById('pechaOptions'),
            publishButton: document.getElementById('publishButton'),
            toastContainer: document.getElementById('toastContainer'),
            metadataContainer: document.querySelector('.metadata-container')
        };

        this.isLoading = false;
        this.metadata = null;
        this.initialize();
    }
    
    async initialize() {
        try {
            await this.loadConfig();
            await this.fetchPechaOptions();
            this.setupEventListeners();
            this.showInitialMetadataState();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize the application. Please refresh the page.', 'error');
        }
    }

    async loadConfig() {
        try {
            const response = await fetch('/config.json');
            if (!response.ok) {
                throw new Error(`Failed to load config: ${response.status} ${response.statusText}`);
            }
            const config = await response.json();
            if (!config.apiEndpoint) {
                throw new Error('API endpoint not found in configuration');
            }
            this.API_ENDPOINT = config.apiEndpoint.replace(/\/$/, ''); // Remove trailing slash if present
        } catch (error) {
            console.error('Config loading error:', error);
            this.showToast('Error loading configuration. Please refresh the page.', 'error');
            throw error; // Re-throw to handle in initialize()
        }
    }

    setupEventListeners() {

        // Listen for changes on the custom dropdown
        this.elements.pechaOptionsContainer.addEventListener('customDropdownChange', () => {
            this.handlePechaSelect();
        });

        this.elements.form.addEventListener('submit', (e) => {
            e.preventDefault();
            this.handlePublish();
        });
    }

    setLoading(isLoading) {
        this.isLoading = isLoading;
        this.elements.pechaSelect.disabled = isLoading;
        this.elements.publishButton.disabled = isLoading;

        if (isLoading) {
            this.elements.publishButton.innerHTML = `
                <div class="button-spinner"></div>
                <span>Publishing...</span>
            `;
        } else {
            this.elements.publishButton.textContent = 'Publish';
        }
    }

    showSelectLoading(isLoading) {
        this.elements.pechaSelect.disabled = isLoading;
        this.elements.publishButton.disabled = isLoading;
        if (isLoading) {
            const loadingOption = document.createElement('option');
            loadingOption.value = '';
            loadingOption.textContent = 'Loading pechas...';
            this.elements.pechaSelect.innerHTML = '';
            this.elements.pechaSelect.appendChild(loadingOption);
        }
    }

    async fetchPechaOptions() {
        this.showSelectLoading(true);
        console.log(this.API_ENDPOINT);
        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    "filter": {
                    },
                    "page": 1,
                    "limit": 100
                })
            });
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const pechas = await response.json();
            this.updatePechaOptions(pechas);
        } catch (error) {
            console.error('Error loading pecha options:', error);
            this.showToast('Unable to load pecha options. Please try again later.', 'error');
        } finally {
            this.showSelectLoading(false);
        }
    }


    updatePechaOptions(pechas) {
        this.elements.pechaSelect.style.display = 'none';
        new CustomSearchableDropdown(this.elements.pechaOptionsContainer, pechas, "selectedPecha");
    }

    async fetchMetadata(pechaId) {
        try {
            // Await the fetch call to get the response
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${pechaId}`, {
                method: 'GET',
                headers: {
                    'accept': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const metadata = await response.json();
            console.log("metata ",metadata)
            this.metadata = metadata;
            return metadata;
        } catch (error) {
            console.error('Error fetching metadata:', error);
            throw error; 
        }
    }

    showInitialMetadataState() {
        this.elements.metadataContainer.innerHTML = `
            <div class="metadata-placeholder">
                <p>Select a pecha to view metadata</p>
            </div>
        `;
    }

    showLoadingState() {
        this.elements.metadataContainer.innerHTML = `
            <div class="metadata-loading">
                <div class="loading-spinner"></div>
                <p>Loading metadata...</p>
            </div>
        `;
    }

    showErrorState(message) {
        this.elements.metadataContainer.innerHTML = `
            <div class="metadata-error">
                <p>${message}</p>
            </div>
        `;
    }

    formatMetadataValue(value) {
        if (value === null || value === undefined) {
            return '<span class="empty-value">N/A</span>';
        }
        if (Array.isArray(value)) {
            if (value.length === 0) return '<span class="empty-value">N/A</span>';

            return value.map(item => {
                const entries = Object.entries(item);
                if (entries.length === 0) return '<span class="empty-value">N/A</span>';

                return entries.map(([lang, text]) => `
            <div class="localized-value">
                <span class="language-tag">${lang}</span>
                <span class="text">${text}</span>
            </div>
        `).join('');
            }).join('');
        }

        if (typeof value === 'object') {
            const entries = Object.entries(value);
            if (entries.length === 0) return '<span class="empty-value">N/A</span>';

            return entries.map(([lang, text]) => `
                <div class="localized-value">
                    <span class="language-tag">${lang}</span>
                    <span class="text">${text}</span>
                </div>
            `).join('');
        }
        return value.toString();
    }

    displayMetadata(metadata) {
        const reorderedMetadata = this.reorderMetadata(metadata);
        const metadataHTML = Object.entries(reorderedMetadata).map(([key, value]) => {
            const formattedKey = key.replace(/_/g, ' ').toUpperCase();
            const formattedValue = this.formatMetadataValue(value);
            return `
                <div class="metadata-item">
                    <div class="metadata-key">${formattedKey}</div>
                    <div class="metadata-value">${formattedValue}</div>
                </div>
            `;
        }).join('');

        this.elements.metadataContainer.innerHTML = `
            <div class="metadata-content">
                ${metadataHTML}
            </div>
        `;
    }
    reorderMetadata(metadata) {
        const order = [
            "title",
            "version_of",
            "commentary_of",
            "translation_of",
            "author",
            "category",
            "long_title",
            "usage_title",
            "alt_titles",
            "language",
            "source",
            "presentation",
            "date",
            "document_id"
        ];
    
        const reorderedMetadata = {};
    
        order.forEach((key) => {
            reorderedMetadata[key] = metadata.hasOwnProperty(key) ? metadata[key] : null;
        });
    
        return reorderedMetadata;
    }
    
    async handlePechaSelect() {
        this.selectedPecha = document.getElementById("selectedPecha");
        const pechaId = this.selectedPecha.dataset.value;

        if (!pechaId) {
            this.showInitialMetadataState();
            return;
        }

        try {
            this.showLoadingState();
            const metadata = await this.fetchMetadata(pechaId);
            this.displayMetadata(metadata);
        } catch (error) {
            console.error('Error in handlePechaSelect:', error);
            this.showToast('Unable to fetch metadata. Please try again later.', 'error');
            this.showErrorState('Failed to load metadata. Please try again.');
        }
    }


    validateFields() {
        if (!this.metadata.category) {
            this.showToast('This pecha does not have category', 'error');
            return false;
        }
        this.selectedPecha = document.getElementById("selectedPecha");
        const publishTextId = this.selectedPecha.dataset.value;
        if (!publishTextId) {
            this.showToast('Please select the pecha OPF', 'error');
            return false;
        }
        const publishDestination = document.querySelector('input[name="destination"]:checked')?.value;
        if(!publishDestination) {
            this.showToast('Please select the publish destination', 'error');
            return false;
        }
        const reserialize = document.querySelector('input[name="reserialize"]:checked')?.value;
        if(!reserialize) {
            this.showToast('Please select the reserialize option', 'error');
            return false;
        }

        return { publishTextId, publishDestination, reserialize: reserialize === 'true' };
    }

    async handlePublish() {
        const validatedData = this.validateFields();
        if (!validatedData) return;

        this.setLoading(true);

        try {
            const { publishTextId, publishDestination, reserialize } = validatedData;
            const response = await fetch(`${this.API_ENDPOINT}/pecha/${publishTextId}/publish`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ destination: publishDestination, reserialize })
            });
            if (!response.ok){
                console.log(response)
                throw new Error(`Unable to publish pecha. Please try again later.`);
            }

            this.showToast('Pecha published successfully', 'success');
            this.elements.form.reset();
            this.showInitialMetadataState();
        } catch (error) {
            console.error('Error publishing:', error);
            this.showToast(`Error: ${error.message}`, 'error');
        } finally {
            this.setLoading(false);
        }
    }

    showToast(message, type) {
        this.clearToasts();

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `${this.getToastIcon(type)} ${message}`;

        this.elements.toastContainer.appendChild(toast);
        setTimeout(() => toast.remove(), 3000);
    }

    getToastIcon(type) {
        switch (type) {
            case 'success':
                return '<i class="fas fa-check-circle"></i>';
            case 'error':
                return '<i class="fas fa-exclamation-circle"></i>';
            default:
                return '<i class="fas fa-info-circle"></i>';
        }
    }

    clearToasts() {
        this.elements.toastContainer.innerHTML = '';
    }
}



document.addEventListener('DOMContentLoaded', () => new UpdateMetaData());