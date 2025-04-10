class UpdateMetaData {
    constructor() {
        this.initialize();
    }

    async initialize() {
        try {
            this.API_ENDPOINT = await loadConfig();
            this.setupElements();
            await this.fetchPechaOptions();
            this.setupEventListeners();
            this.showInitialMetadataState();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }

    setupElements() {
        this.elements = {
            form: document.getElementById('updateForm'),
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            googleDocsContainer: document.getElementById('googleDocsContainer'),
            docsInput: document.getElementById('googleDocsInput'),
            updateButton: document.getElementById('updateButton'),
            buttonText: document.querySelector('.button-text'),
            spinner: document.querySelector('.spinner'),
            toastContainer: document.getElementById('toastContainer'),
            formGroups: document.querySelectorAll('.form-group'),
            metadataContainer: document.querySelector('.metadata-container'),
            updateFormContainer: document.getElementById('updateFormContainer')
        };

        this.isLoading = false;
    }

    setupEventListeners() {
        // Listen for changes on the custom dropdown
        this.elements.pechaOptionsContainer.addEventListener('customDropdownChange', () => {
            this.handlePechaSelect();
        });

        this.elements.form.addEventListener('submit', (e) => {
            e.preventDefault();
            if (!this.isLoading) {
                this.handleSubmit(e);
            }
        });
    }

    setLoadingState(loading) {
        this.isLoading = loading;
        this.elements.updateButton.disabled = loading;
        this.elements.buttonText.textContent = loading ? 'Updating...' : 'Submit';
        this.elements.spinner.style.display = loading ? 'inline-block' : 'none';

        this.elements.formGroups.forEach(group => {
            group.classList.toggle('disabled', loading);
        });
    }

    async fetchPechaOptions() {
        this.showSpinner(this.elements.pechaOptionsContainer, true);
        this.hideInputs();
        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({"filter": {
                    },
                    "page": 1,
                    "limit": 100
                  })
            });

            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

            const pechas = await response.json();
            console.log(":::::",pechas)
            this.updatePechaOptions(pechas);
        } catch (error) {
            console.error('Error loading pecha options:', error);
            this.elements.pechaOptionsContainer.innerHTML="Unable to load pecha options. Please try again later.";
            this.showToast('Unable to load pecha options. Please try again later.', 'error');
        } finally {
            this.showSpinner(this.elements.pechaOptionsContainer, false);
        }
    }

    updatePechaOptions(pechas) {
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
            this.hideInputs();
            this.showInitialMetadataState();
            return;
        }

        try {
            this.showLoadingState();
            const metadata = await this.fetchMetadata(pechaId);
            this.displayMetadata(metadata);
            this.showInputs();
        } catch (error) {
            console.error('Error in handlePechaSelect:', error);
            this.showToast('Unable to fetch metadata. Please try again later.', 'error');
            this.showErrorState('Failed to load metadata. Please try again.');
            this.hideInputs();
        }
    }

    validateFields() {
        this.selectedPecha = document.getElementById("selectedPecha");
        const publishTextId = this.selectedPecha.dataset.value;
        const googleDocLink = this.elements.docsInput.value.trim();

        if (!publishTextId) {
            this.showToast('Please select the published text', 'warning');
            return false;
        }

        const docId = this.extractDocIdFromLink(googleDocLink);
        if (!docId) {
            this.showToast('Enter valid Google Docs link', 'warning');
            return false;
        }

        return { publishTextId, docId };
    }

    async handleSubmit(e) {
        const validatedData = this.validateFields();
        if (!validatedData) return;

        this.setLoadingState(true);

        try {
            const { publishTextId, docId } = validatedData;
            const blob = await downloadDoc(docId);
            if (!blob) {
                this.showToast("Failed to download document", "error");
                throw new Error('Failed to download document');
            }

            await this.uploadDocument(publishTextId, blob, docId);
            this.showToast('Document updated successfully!', 'success');
            this.elements.form.reset();
            this.showInitialMetadataState();
            this.hideInputs();
        } catch (error) {
            console.error('Error during update:', error);
            this.showToast(`Error: ${error.message}`, 'error');
        } finally {
            this.setLoadingState(false);
        }
    }

    async uploadDocument(publishTextId, blob, docId) {
        const formData = new FormData();
        formData.append('text', blob, `text_${docId}.docx`);

        const response = await fetch(`${this.API_ENDPOINT}/text/${publishTextId}`, {
            method: 'PUT',
            body: formData
        });

        if (!response.ok) {
            const errorText = await response.text();
            this.showToast("Update failed", "error");
            throw new Error(`Update failed: ${errorText}`);
        }
        return response;
    }

    extractDocIdFromLink(docLink) {
        const regex = /\/d\/([a-zA-Z0-9-_]+)/;
        const match = docLink.match(regex);
        return match?.[1] || null;
    }

    hideInputs() {
        this.elements.googleDocsContainer.style.display = 'none';
        // this.elements.updateButton.style.display = 'none';
    }

    showInputs() {
        this.elements.googleDocsContainer.style.display = 'block';
        // this.elements.updateButton.style.display = 'block';
    }

    showToast(message, type) {
        this.clearToasts();

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;

        this.elements.toastContainer.appendChild(toast);
        setTimeout(() => toast.remove(), 3000);
    }

    showSpinner(parentNode, show) {
        if (show) {
            parentNode.innerHTML = '';
            const spinner = document.createElement('div');
            spinner.className = 'spinner';
            spinner.style.display = 'inline-block';
            parentNode.appendChild(spinner);
        } else {
            const spinner = parentNode.querySelector('.spinner');
            if (spinner) {
                spinner.remove();
            }
        }
    }

    clearToasts() {
        this.elements.toastContainer.innerHTML = '';
    }

}

document.addEventListener('DOMContentLoaded', () => new UpdateMetaData());