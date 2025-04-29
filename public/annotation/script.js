class AnnotationForm {
    constructor() {
        this.form = document.getElementById('annotationForm');
        this.annotationSelect = document.getElementById('annotation');
        this.pechaSelect = document.getElementById('pecha');
        this.pechaDropdownLabel = document.getElementById('pechaDropdownLabel');
        this.pechaDropdown = document.getElementById('pechaDropdown');
        this.segmentationLayer = document.getElementById('segmentationLayer');
        this.annotationTitle = document.getElementById('annotationTitle');
        this.googleDocsUrl = document.getElementById('googleDocsUrl');
        this.toastContainer = document.getElementById('toastContainer');
        this.pechaLoadingSpinner = document.getElementById('pechaLoadingSpinner');
        this.formLoadingSpinner = document.getElementById('formLoadingSpinner');

        // Search-related elements
        this.searchContainers = document.querySelectorAll('.select-search-container');

        this.metadata = null;
        // Bind methods to maintain 'this' context
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleAnnotationChange = this.handleAnnotationChange.bind(this);
        this.initializeForm = this.initializeForm.bind(this);
        this.initializeSearchUI = this.initializeSearchUI.bind(this);
        this.toggleLoadingSpinner = this.toggleLoadingSpinner.bind(this);

        // Initialize event listeners
        this.initialize()
    }

    async initialize() {
        try {
            this.API_ENDPOINT = await getApiEndpoint();
            this.setupEventListeners();
            this.initializeForm();
            this.initializeSearchUI();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }
    setupEventListeners() {
        this.form.addEventListener('submit', this.handleSubmit);
        this.pechaSelect.addEventListener('change', (e) => this.onPechaSelect(e.target.value));
        this.annotationSelect.addEventListener('change', this.handleAnnotationChange);
    }

    // New method to initialize search functionality
    initializeSearchUI() {
        this.searchContainers.forEach(container => {
            const select = container.querySelector('select');
            const searchOverlay = container.querySelector('.search-overlay');
            const searchInput = container.querySelector('.search-input');
            const searchResults = container.querySelector('.search-results');

            // Prevent the native dropdown from showing
            select.addEventListener('mousedown', (e) => {
                e.preventDefault();
                searchOverlay.classList.toggle('active');
                if (searchOverlay.classList.contains('active')) {
                    searchInput.focus();
                    this.populateSearchResults(select, searchResults, searchInput.value);
                }
            });

            // Close search overlay when clicking outside
            document.addEventListener('click', (e) => {
                if (!container.contains(e.target)) {
                    searchOverlay.classList.remove('active');
                }
            });

            // Search functionality
            searchInput.addEventListener('input', () => {
                this.populateSearchResults(select, searchResults, searchInput.value);
            });

            // Select an option from search results
            searchResults.addEventListener('click', (e) => {
                if (e.target.classList.contains('search-item')) {
                    const value = e.target.dataset.value;
                    select.value = value;

                    // Trigger change event
                    const changeEvent = new Event('change', { bubbles: true });
                    select.dispatchEvent(changeEvent);

                    searchOverlay.classList.remove('active');
                }
            });

            // Handle keyboard navigation
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

                        // Trigger change event
                        const changeEvent = new Event('change', { bubbles: true });
                        select.dispatchEvent(changeEvent);

                        searchOverlay.classList.remove('active');
                    }
                }
            });
        });
    }

    // Helper method to populate search results
    populateSearchResults(select, resultsContainer, searchTerm) {
        resultsContainer.innerHTML = '';
        searchTerm = searchTerm.toLowerCase();

        Array.from(select.options).forEach(option => {
            if (option.value && (searchTerm === '' || option.text.toLowerCase().includes(searchTerm))) {
                const item = document.createElement('div');
                item.className = 'search-item';
                item.textContent = option.text;
                item.dataset.value = option.value;

                if (option.value === select.value) {
                    item.classList.add('selected');
                }

                resultsContainer.appendChild(item);
            }
        });
    }

    // Helper method for keyboard navigation in search results
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

    toggleLoadingSpinner(isLoading) {
        if (isLoading) {
            // Show loading spinner
            this.pechaLoadingSpinner.classList.add('active');
            
            // Hide dropdown containers while loading
            this.searchContainers.forEach(container => {
                container.classList.add('loading');
            });
        } else {
            // Hide loading spinner
            this.pechaLoadingSpinner.classList.remove('active');
            
            // Show dropdown containers after loading
            this.searchContainers.forEach(container => {
                container.classList.remove('loading');
            });
        }
    }

    async toggleConditionalFields() {
        const isCommentaryOrTranslation = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);

        const isAlignment = this.annotationSelect?.value === 'Alignment';

        if (!isCommentaryOrTranslation || !isAlignment) {
            this.pechaDropdown.value = "";
            this.segmentationLayer.value = "";
        }
        const fields = {
            'pechaField': isCommentaryOrTranslation && isAlignment,
            'segmentationField': isAlignment && isCommentaryOrTranslation
        };

        Object.entries(fields).forEach(([fieldId, shouldShow]) => {
            document.getElementById(fieldId).style.display = shouldShow ? 'block' : 'none';
        });
        
        if(isCommentaryOrTranslation && isAlignment) {
            // Set the label dynamically based on relationship type
            if ('translation_of' in this.metadata && this.metadata.translation_of !== null) {
                this.pechaDropdownLabel.textContent = 'Translation of';
                const pechaId = this.metadata.translation_of;
                this.pechaDropdown.value = pechaId;
            } else if ('commentary_of' in this.metadata && this.metadata.commentary_of !== null) {
                this.pechaDropdownLabel.textContent = 'Commentary of';
                const pechaId = this.metadata.commentary_of;
                this.pechaDropdown.value = pechaId;
            }
            this.segmentationLayer.innerHTML = '<option value="">Select Segmentation Layer</option>';
            this.segmentationLayer.remove(1)
            this.annotations.forEach(annotation => {
                const option = document.createElement('option');
                option.value = annotation.path;
                option.textContent = annotation.title;
                this.segmentationLayer.appendChild(option);
            });            
        }
        
        // Reset validation state
        this.form.classList.remove('was-validated');
    }


    async fetchMetadata(pechaId){
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
            console.log("meta ",metadata)
            return metadata;
        } catch (error) {
            console.error('Error fetching metadata:', error);
            this.showToast("Unable to fetch metadata", 'error');
            return null;
        }
    }

    async getAnnotation(pechaId) {
        const url = `${this.API_ENDPOINT}/annotation/${pechaId}`;
      
        try {
          const response = await fetch(url, {
            method: 'GET',
            headers: {
              'accept': 'application/json',
            },
          });
      
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
      
          const data = await response.json();
          console.log(data); // You can modify this to return data instead of logging
          return data;
        } catch (error) {
          console.error('Error fetching annotation:', error);
          throw error;
        }
      }

    extractAnnotations(data) {
    return Object.entries(data).map(([id, details]) => ({
        path: details.path,
        title: details.title
    }));
    }

    async onPechaSelect(pechaId) {
        this.metadata = await this.fetchMetadata(pechaId);
        console.log("metadata fetched : ",this.metadata)
        const isCommentaryOrTranslation = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);
        const isAlignment = this.annotationSelect?.value === 'Alignment';
        if (isCommentaryOrTranslation && isAlignment) {
            
        const annotations = await this.getAnnotation(this.metadata.commentary_of);
        this.annotations = this.extractAnnotations(annotations);
        console.log("annotation:",this.annotations)
        }
        this.toggleConditionalFields();
    }

    handleAnnotationChange(event) {
        this.toggleConditionalFields();
    }

    async fetchPechaList(filterBy) {
        let body = { filter: {} };
        const filters = {
            "commentary_of": {
                "and": [
                    { "field": "commentary_of", "operator": "==", "value": null },
                    { "field": "translation_of", "operator": "==", "value": null }
                ]
            },
            "version_of": {
                "and": [
                    { "field": "commentary_of", "operator": "==", "value": null },
                    { "field": "version_of", "operator": "==", "value": null }
                ]
            },
            "translation_of": {
                "field": "language",
                "operator": "==",
                "value": "bo"
            }
        };

        body.filter = filters[filterBy] || {};

        try {
            // Show loading spinner
            this.toggleLoadingSpinner(true);
            
            let allPechas = [];
            let currentPage = 1;
            let hasMorePages = true;
            const limit = 100; // Keep the same limit per request
            
            // Loop until we've fetched all pages
            while (hasMorePages) {
                body.page = currentPage;
                body.limit = limit;
                
                const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                    method: 'POST',
                    headers: {
                        'accept': 'application/json',
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(body)
                });
                if (!response.ok) {
                    throw new Error(`Failed to fetch data: ${response.statusText}`);
                }
                const pechas = await response.json();
                allPechas = allPechas.concat(pechas.metadata);
                hasMorePages = pechas.metadata.length === limit;
                currentPage++;
            }
            
            // Hide loading spinner
            this.toggleLoadingSpinner(false);
            return allPechas;
        } catch (error) {
            // Hide loading spinner on error
            this.toggleLoadingSpinner(false);
            console.error("Error loading pecha options:", error);
            this.showToast("Unable to load pecha options. Please try again later.", 'error');
            return [];
        }
    }

    populatePechaDropdowns(pechas) {
        // Clear existing options first (keep only the default one)
        while (this.pechaSelect.options.length > 1) {
            this.pechaSelect.remove(1);
        }
        while (this.pechaDropdown.options.length > 1) {
            this.pechaDropdown.remove(1);
        }

        pechas.forEach(pecha => {
            const option = new Option(`${pecha.id} - ${pecha.title["bo"]}`, pecha.id);
            this.pechaSelect.add(option.cloneNode(true));
            this.pechaDropdown.add(option);
        });
    }

    async initializeForm() {
        try {
            // Show loading spinner before fetching pechas
            this.toggleLoadingSpinner(true);
            const pechas = await this.fetchPechaList();
            console.log("pecha:::", pechas);
            this.populatePechaDropdowns(pechas);
            // Loading spinner is already hidden in fetchPechaList
        } catch (error) {
            // Hide loading spinner in case of error
            this.toggleLoadingSpinner(false);
            console.error('Error initializing form:', error);
            this.showToast('Failed to initialize form. Please refresh the page.', 'error');
        }
    }

    getFormData() {
        // Create FormData from the form (this won't include disabled fields)
        const formData = new FormData(this.form);
        const data = Object.fromEntries(formData.entries());
        
        // Manually add values from disabled fields
        const disabledFields = this.form.querySelectorAll('input:disabled, select:disabled, textarea:disabled');
        disabledFields.forEach(field => {
            if (field.name && field.value) {
                data[field.name] = field.value;
            }
        });

        // Format the data according to the required structure
        const formattedData = {
            pecha_id: data.pecha,
            type: data.annotation_type,
            title: data.annotation_title,
            document_id: this.extractGoogleDocsId(data.google_docs_id)
        };

        // Handle pecha_aligned_to based on whether it's a root pecha or not
        formattedData.aligned_to = data.pechaDropdown ? {
                pecha_id: data.pechaDropdown,
                alignment_id: data.segmentationLayer || null
            } : null;

        return formattedData;
    }

    extractGoogleDocsId(url) {
        // Extract Google Docs ID from URL
        const match = url.match(/\/d\/([^\/]+)/);
        return match && match[1] ? match[1] : url;
    }

    async submitAnnotation(formData) {
        const response = await fetch(`${this.API_ENDPOINT}/annotation/`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            throw new Error('Failed to add annotation');
        }

        return response.json();
    }

    async handleSubmit(event) {
        event.preventDefault();
        // Disable submit button and show spinner inside
        const submitBtn = document.getElementById('submitAnnotationBtn');
        const btnText = submitBtn?.querySelector('.btn-text');
        const btnSpinner = submitBtn?.querySelector('.btn-spinner');
        if (submitBtn) submitBtn.disabled = true;
        if (btnText) btnText.style.display = 'none';
        if (btnSpinner) btnSpinner.style.display = 'inline-block';
        try {
            const data = this.getFormData();
            console.log("data ::: ", data)
            const isValid = this.validateForm(data);
            if (!isValid) {
                if (submitBtn) submitBtn.disabled = false;
                if (btnText) btnText.style.display = '';
                if (btnSpinner) btnSpinner.style.display = 'none';
                return;
            }
            // Fetch document and prepare form data concurrently
            const blob = await downloadDoc(data.document_id).catch(err => {
                throw new Error(`Download failed: ${err.message}`);
            });

            const formData = await this.prepareFormData(blob, data);
            const response = await this.submitAnnotation(formData);
            console.log("response:::", response);

            this.showToast('Annotation added successfully!', 'success');
            this.resetForm();
        } catch (error) {
            this.showToast(error.message, 'error');
            console.error('Error adding annotation:', error);
        } finally {
            if (submitBtn) submitBtn.disabled = false;
            if (btnText) btnText.style.display = '';
            if (btnSpinner) btnSpinner.style.display = 'none';
        }
    }

    // Helper methods to publish
    async prepareFormData(blob, annotation) {
        const formData = new FormData();
        formData.append("document", blob, `document_${annotation.document_id}.docx`);
        formData.append("annotation", JSON.stringify(annotation));
        return formData;
    }

    showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `${this.getToastIcon(type)} ${message}`;
        this.toastContainer.appendChild(toast);

        setTimeout(() => {
            toast.remove();
        }, 3000);
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

    highlightError(field) {
        field.classList.add('error');
    }

    validateForm(data) {
        if (!data.pecha_id) {
            this.highlightError(this.pechaSelect);
            this.showToast('Pecha is required', 'error');
            return false;
        }
        const isCommentaryOrTranslation = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);

        const isAlignment = this.annotationSelect?.value === 'alignment';

        if (isCommentaryOrTranslation && isAlignment) {
            if (!data.aligned_to.alignment_annotation) {
                this.highlightError(this.segmentationLayer);
                this.showToast('Alignment annotation is required', 'error');
                return false;
            }
        }
        
        if (!data.title) {
            this.highlightError(this.annotationTitle);
            this.showToast('Annotation Title is required', 'error');
            return false;
        }

        if (!data.document_id) {
            this.highlightError(this.googleDocsUrl);
            this.showToast('Google Docs URL is required', 'error');
            return false;
        }

        return true;
    }

    resetForm() {
        this.form.reset();
        this.toggleConditionalFields('');
    }

}

// Initialize the form when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new AnnotationForm();
});
