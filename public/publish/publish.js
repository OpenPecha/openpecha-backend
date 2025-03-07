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

        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.isLoading = false;
        this.fetchPechaOptions();
        this.setupEventListeners();
        this.showInitialMetadataState();
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

        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({})
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
        const metadataHTML = Object.entries(metadata).map(([key, value]) => {
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
        this.selectedPecha = document.getElementById("selectedPecha");
        const publishTextId = this.selectedPecha.dataset.value;
        if (!publishTextId) {
            this.showToast('Please select the published text', 'warning');
            return false;
        }

        return { publishTextId };
    }

    async handlePublish() {
        const validatedData = this.validateFields();
        if (!validatedData) return;

        this.setLoading(true);

        try {
            const { publishTextId } = validatedData;
            const response = await fetch(`${this.API_ENDPOINT}/pecha/${publishTextId}/publish`, {
                method: 'POST'
            });
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

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
        toast.textContent = message;

        this.elements.toastContainer.appendChild(toast);
        setTimeout(() => toast.remove(), 3000);
    }

    clearToasts() {
        this.elements.toastContainer.innerHTML = '';
    }
}



document.addEventListener('DOMContentLoaded', () => new UpdateMetaData());