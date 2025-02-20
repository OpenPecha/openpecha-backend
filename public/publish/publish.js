class UpdateMetaData {
    constructor() {
        this.elements = {
            form: document.getElementById('publishForm'),
            pechaSelect: document.getElementById('pechaOptions'),
            publishButton: document.getElementById('publishButton'),
            toastContainer: document.getElementById('toastContainer'),
            metadataContainer: document.querySelector('.metadata-container')
        };

        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.setupEventListeners();
        this.fetchPechaOptions();
        this.showInitialMetadataState();
    }

    setupEventListeners() {
        this.elements.pechaSelect.addEventListener('change', () => this.handlePechaSelect());

        this.elements.form.addEventListener('submit', (e) => {
            e.preventDefault();
            this.handlePublish();
        });
    }

    async fetchPechaOptions() {
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
        }
    }

    async handlePechaSelect() {
        const pechaId = this.elements.pechaSelect.value;

        if (!pechaId) {
            console.log("No pechaId selected");
            return;
        }

        try {
            const metadata = await this.fetchMetadata(pechaId);
            console.log('Metadata received:', metadata);
            // You can now use the metadata as needed
        } catch (error) {
            console.error('Error in handlePechaSelect:', error);
            this.showToast('Unable to fetch metadata. Please try again later.', 'error');
        }
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
        const pechaId = this.elements.pechaSelect.value;

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
    
    updatePechaOptions(pechas) {
        // Clear existing options except the first one
        this.elements.pechaSelect.innerHTML = '<option value="">Select pecha</option>';

        // Use DocumentFragment for better performance
        const fragment = document.createDocumentFragment();

        pechas.forEach(({ id, title }) => {
            const option = document.createElement('option');
            option.value = id;
            option.textContent = `(${id}) ${title}`;
            fragment.appendChild(option);
        });

        this.elements.pechaSelect.appendChild(fragment);
    }

    validateFields() {
        const publishTextId = this.elements.pechaSelect.value.trim();

        if (!publishTextId) {
            this.showToast('Please select the published text', 'warning');
            return false;
        }

        return { publishTextId };
    }

    async handlePublish() {
        const validatedData = this.validateFields();
        if (!validatedData) return;

        try {
            const { publishTextId } = validatedData;
            const response = await fetch(`${this.API_ENDPOINT}/pecha/${publishTextId}/publish`, {
                method: 'POST'
            });
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            this.showToast('Pecha published successfully', 'success');
            this.elements.form.reset();
        } catch (error) {
            console.error('Error publishing:', error);
            this.showToast(`Error: ${error.message}`, 'error');
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