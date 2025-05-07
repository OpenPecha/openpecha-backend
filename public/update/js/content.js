class UpdateMetaData {
    constructor() {
        this.initialize();
    }

    setupElements() {
        this.elements = {
            form: document.getElementById('updateForm'),
            searchContainers: document.querySelectorAll('.select-search-container'),
            pechaSelect: document.getElementById('pecha'),
            googleDocsContainer: document.getElementById('googleDocsContainer'),
            docsInput: document.getElementById('googleDocsInput'),
            updateButton: document.getElementById('updateButton'),
            buttonText: document.querySelector('.button-text'),
            spinner: document.querySelector('.spinner'),
            toastContainer: document.getElementById('toastContainer'),
            formGroups: document.querySelectorAll('.form-group'),
            metadataContainer: document.querySelector('.metadata-container'),
            updateFormContainer: document.getElementById('updateFormContainer'),
            pechaLoadingSpinner: document.getElementById('pechaLoadingSpinner'),
            annotationAlignmentGroup: document.getElementById('annotationAlignmentGroup'),
            annotationAlignmentSelect: document.getElementById('annotationAlignment'),
            annotationOptionsContainer: document.getElementById('annotationAlignmentContainer'),
            annotationLoadingSpinner: document.getElementById('annotationLoadingSpinner')
        };

        this.isLoading = false;
    }

    async initialize() {
        try {
            this.API_ENDPOINT = await getApiEndpoint();
            this.setupElements();
            await this.fetchPechaOptions();
            this.setupEventListeners();
            this.showInitialMetadataState();
            this.initializeSearchUI();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }

    initializeSearchUI() {
        this.elements.searchContainers.forEach(container => {
            const select = container.querySelector('select');
            const searchOverlay = container.querySelector('.search-overlay');
            const searchInput = container.querySelector('.search-input');
            const searchResults = container.querySelector('.search-results');
            // Toggle search overlay when clicking on the select
            select.addEventListener('mousedown', (e) => {
                e.preventDefault();
                searchOverlay.classList.toggle('active');
                if (searchOverlay.classList.contains('active')) {
                    searchInput.focus();
                    this.populateSearchResults(select, searchResults, searchInput.value);
                }
            });

            // Close overlay when clicking outside
            document.addEventListener('click', (e) => {
                if (!container.contains(e.target)) {
                    searchOverlay.classList.remove('active');
                }
            });

            // Filter results when typing
            searchInput.addEventListener('input', () => {
                this.populateSearchResults(select, searchResults, searchInput.value);
            });

            // Handle item selection
            searchResults.addEventListener('click', (e) => {
                if (e.target.classList.contains('search-item')) {
                    const value = e.target.dataset.value;
                    select.value = value;

                    const changeEvent = new Event('change', { bubbles: true });
                    select.dispatchEvent(changeEvent);

                    searchOverlay.classList.remove('active');
                }
            });

            // Keyboard navigation
            searchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') {
                    searchOverlay.classList.remove('active');
                } else if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    this.navigateSearchResults(searchResults, 'down');
                } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    this.navigateSearchResults(searchResults, 'up');
                } else if (e.key === 'Enter') {
                    e.preventDefault();
                    const selectedItem = searchResults.querySelector('.search-item.selected');
                    if (selectedItem) {
                        const value = selectedItem.dataset.value;
                        select.value = value;

                        const changeEvent = new Event('change', { bubbles: true });
                        select.dispatchEvent(changeEvent);

                        searchOverlay.classList.remove('active');
                    }
                }
            });
        });
    }

    populateSearchResults(select, resultsContainer, searchTerm) {
        resultsContainer.innerHTML = '';
        const options = Array.from(select.options).slice(1); // Skip the placeholder
        const lowercaseSearchTerm = searchTerm.toLowerCase();
        
        options.forEach(option => {
            if (!searchTerm || option.text.toLowerCase().includes(lowercaseSearchTerm)) {
                const item = document.createElement('div');
                item.className = 'search-item';
                item.textContent = option.text;
                item.dataset.value = option.value;
                resultsContainer.appendChild(item);
            }
        });
        
        // Select the first item by default
        const firstItem = resultsContainer.querySelector('.search-item');
        if (firstItem) {
            firstItem.classList.add('selected');
        }
    }

    navigateSearchResults(resultsContainer, direction) {
        const items = resultsContainer.querySelectorAll('.search-item');
        if (items.length === 0) return;

        const selectedItem = resultsContainer.querySelector('.search-item.selected');
        let nextIndex = 0;

        if (selectedItem) {
            const currentIndex = Array.from(items).indexOf(selectedItem);
            selectedItem.classList.remove('selected');

            if (direction === 'down') {
                nextIndex = (currentIndex + 1) % items.length;
            } else {
                nextIndex = (currentIndex - 1 + items.length) % items.length;
            }
        }

        items[nextIndex].classList.add('selected');
        items[nextIndex].scrollIntoView({ block: 'nearest' });
    }

    setupEventListeners() {
        this.elements.pechaSelect.addEventListener('change', () => this.handlePechaSelect());
        this.elements.form.addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleSubmit(e);
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

    toggleAnnotationLoadingSpinner(isLoading) {
        if (isLoading) {
            this.elements.annotationLoadingSpinner.style.display = 'flex';
            this.elements.annotationOptionsContainer.style.display = 'none';
        } else {
            this.elements.annotationLoadingSpinner.style.display = 'none';
            this.elements.annotationOptionsContainer.style.display = 'block';
        }
    }

    populateAnnotationDropdown(annotations) {
        // Clear existing options except the first one
        while (this.elements.annotationAlignmentSelect.options.length > 1) {
            this.elements.annotationAlignmentSelect.remove(1);
        }
        
        // if (annotations.length === 0) {
        //     this.elements.annotationAlignmentGroup.style.display = 'none';
        //     return;
        // }
        
        annotations.forEach(annotation => {
            const option = new Option(annotation.title, annotation.id);
            this.elements.annotationAlignmentSelect.add(option.cloneNode(true));
        });
    }

    async fetchPechaOptions() {
        this.toggleLoadingSpinner(true);
        this.hideInputs();
        try {
            let allPechas = [];
            let currentPage = 1;
            let hasMorePages = true;
            const limit = 100; 
            
            while (hasMorePages) {
                const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                    method: 'POST',
                    headers: {
                        'accept': 'application/json',
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        "filter": {},
                        "page": currentPage,
                        "limit": limit
                    })
                });

                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

                const pechas = await response.json();
                allPechas = allPechas.concat(pechas.metadata);
                hasMorePages = pechas.metadata.length === limit;
                currentPage++;
            }
            
            this.populatePechaDropdown(allPechas);
        } catch (error) {
            console.error('Error loading pecha options:', error);
            this.showToast('Unable to load pecha options. Please try again later.', 'error');
        } finally {
            this.toggleLoadingSpinner(false);
        }
    }

    toggleLoadingSpinner(isLoading) {
        if (isLoading) {
            this.elements.pechaLoadingSpinner.classList.add('active');
            
            this.elements.searchContainers.forEach(container => {
                container.classList.add('loading');
            });
        } else {
            this.elements.pechaLoadingSpinner.classList.remove('active');
            
            this.elements.searchContainers.forEach(container => {
                container.classList.remove('loading');
            });
        }
    }

    populatePechaDropdown(pechas) {
        while (this.elements.pechaSelect.options.length > 1) {
            this.elements.pechaSelect.remove(1);
        }
        pechas.forEach(pecha => {
            const title = pecha.title.bo ?? pecha.title[pecha.language];
            const option = new Option(`${pecha.id} - ${title}`, pecha.id);
            this.elements.pechaSelect.add(option.cloneNode(true));
        });
    }

    async fetchAnnotations(pechaId) {
        try {
            const response = await fetch(`${this.API_ENDPOINT}/annotation/${pechaId}`, {
                method: 'GET',
                headers: {
                    'accept': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const annotations = await response.json();
            return annotations;
        } catch (error) {
            console.error('Error fetching annotations:', error);
            this.showToast('Unable to fetch annotations. Please try again later.', 'error');
            return {};
        }
    }

    extractAnnotations(data) {
        return Object.entries(data).map(([id, details]) => ({
            id,
            title: details.title
        }));
    }

    async fetchMetadata(pechaId) {
        try {
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
            if (!value)
                return
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
            "language",
            "author",
            "category",
            "source",
            "long_title",
            "document_id",
            "usage_title",
            "alt_titles",
            "presentation",
            "date"
        ];
    
        const reorderedMetadata = {};
    
        order.forEach((key) => {
            reorderedMetadata[key] = metadata.hasOwnProperty(key) ? metadata[key] : null;
        });
    
        return reorderedMetadata;
    }

    async handlePechaSelect() {

        const pechaId = this.elements.pechaSelect.value;

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
            
            // Fetch and populate annotation alignments
            this.toggleAnnotationLoadingSpinner(true);
            try {
                const annotations = await this.fetchAnnotations(pechaId);
                const extractedAnnotations = this.extractAnnotations(annotations);
                this.populateAnnotationDropdown(extractedAnnotations);
            } catch (error) {
                console.error('Error fetching annotations:', error);
                this.elements.annotationAlignmentGroup.style.display = 'none';
            } finally {
                this.toggleAnnotationLoadingSpinner(false);
            }
            
        } catch (error) {
            console.error('Error fetching metadata:', error);
            this.showErrorState(error.message);
        }
    }

    validateFields() {
        const pechaId = this.elements.pechaSelect.value;
        const googleDocLink = this.elements.docsInput.value.trim();
        const annotation_id = this.elements.annotationAlignmentSelect.value;
        
        if (!pechaId) {
            this.showToast('Please select the published text', 'warning');
            return false;
        }
        if(!annotation_id){
            this.showToast('Please select an annotation', 'warning');
            return false;
        }
        const docId = this.extractDocIdFromLink(googleDocLink);
        if (!docId) {
            this.showToast('Enter valid Google Docs link', 'warning');
            return false;
        }

        return { pechaId, docId, annotation_id };
    }

    async handleSubmit(e) {
        const validatedData = this.validateFields();
        if (!validatedData) return;
        this.setLoadingState(true);
        
        try {
            const { pechaId, docId, annotation_id } = validatedData;
            const blob = await downloadDoc(docId);
            if (!blob) {
                this.showToast("Failed to download document", "error");
                throw new Error('Failed to download document');
            }
            
            await this.uploadDocument(pechaId, blob, docId, annotation_id);
            this.showToast('Document updated successfully!', 'success');
            this.elements.form.reset();
            this.showInitialMetadataState();
            this.hideInputs();
            this.elements.annotationAlignmentGroup.style.display = 'none';
        } catch (error) {
            console.error('Error during update:', error);
            this.showToast(`Error: ${error.message}`, 'error');
        } finally {
            this.setLoadingState(false);
        }
    }

    async uploadDocument(pechaId, blob, docId, annotation_id) {
        const formData = new FormData();
        formData.append('text', blob, `text_${docId}.docx`);
        
        if (annotation_id) {
            formData.append('annotation_id', annotation_id);
        }
        
        const response = await fetch(`${this.API_ENDPOINT}/text/${pechaId}`, {
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
        this.elements.annotationAlignmentGroup.style.display = 'none';
    }

    showInputs() {
        this.elements.googleDocsContainer.style.display = 'block';
        this.elements.annotationAlignmentGroup.style.display = 'block';
    }
    
    hideAnnotationField() {
        this.elements.annotationAlignmentGroup.style.display = 'none';
    }
    
    showAnnotationField() {
        this.elements.annotationAlignmentGroup.style.display = 'block';
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