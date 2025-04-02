class AnnotationForm {
    constructor() {
        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.form = document.getElementById('annotationForm');
        this.annotationSelect = document.getElementById('annotation');
        this.pechaSelect = document.getElementById('pecha');
        this.pechaDropdown = document.getElementById('pechaDropdown');
        this.segmentationLayer = document.getElementById('segmentationLayer');
        this.segmentationTitle = document.getElementById('segmentationTitle');
        this.googleDocsUrl = document.getElementById('googleDocsUrl');
        this.toastContainer = document.getElementById('toastContainer');

        // Search-related elements
        this.searchContainers = document.querySelectorAll('.select-search-container');

        this.metadata = null;
        // Bind methods to maintain 'this' context
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleAnnotationChange = this.handleAnnotationChange.bind(this);
        this.initializeForm = this.initializeForm.bind(this);
        this.initializeSearchUI = this.initializeSearchUI.bind(this);

        // Initialize event listeners
        this.setupEventListeners();
        this.initializeForm();
        this.initializeSearchUI();
    }

    setupEventListeners() {
        this.form.addEventListener('submit', this.handleSubmit);
        this.pechaSelect.addEventListener('change', (e) => this.fetchMetadata(e.target.value));
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

    async toggleConditionalFields() {
        const isPechaCommentary = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);

        const isSegmentation = this.annotationSelect?.value === 'Segmentation';

        if (!isPechaCommentary || !isSegmentation) {
            this.pechaDropdown.value = "";
            this.segmentationLayer.value = "";
        }
        const fields = {
            'pechaField': isPechaCommentary && isSegmentation,
            'segmentationField': isSegmentation && isPechaCommentary
        };

        Object.entries(fields).forEach(([fieldId, shouldShow]) => {
            document.getElementById(fieldId).style.display = shouldShow ? 'block' : 'none';
        });

        // Reset validation state
        this.form.classList.remove('was-validated');
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
            this.metadata = metadata;
        } catch (error) {
            console.error('Error fetching metadata:', error);
            throw error;
        } finally {
            this.toggleConditionalFields();
        }
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
            return pechas;
        } catch (error) {
            this.handleSpinner(this.pechaOptionsContainer, false);
            console.error("Error loading pecha options:", error);
            alert("Unable to load pecha options. Please try again later.");
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
            const option = new Option(`${pecha.id} - ${pecha.title}`, pecha.id);
            this.pechaSelect.add(option.cloneNode(true));
            this.pechaDropdown.add(option);
        });
    }

    async initializeForm() {
        try {
            const pechas = await this.fetchPechaList("version_of");
            this.populatePechaDropdowns(pechas);
        } catch (error) {
            console.error('Error initializing form:', error);
        }
    }

    getFormData() {
        const formData = new FormData(this.form);
        return Object.fromEntries(formData.entries());
    }

    resetForm() {
        this.form.reset();
        this.toggleConditionalFields('');
    }

    validateForm(data) {
        if (!data.pecha) {
            this.highlightError(this.pechaSelect);
            this.showToast('Pecha is required', 'error');
            return false;
        }
        if (!data.annotation) {
            this.highlightError(this.annotationSelect);
            this.showToast('Annotation is required', 'error');
            return false;
        }
        if (!data.pechaDropdown) {
            this.highlightError(this.pechaDropdown);
            this.showToast('Pecha is required', 'error');
            return false;
        }
        // if (!data.segmentationLayer) {
        //     this.highlightError(this.segmentationLayer);
        //     this.showToast('Segmentation Layer is required', 'error');
        //     return false;
        // }

        if (!data.segmentationTitle) {
            this.highlightError(this.segmentationTitle);
            this.showToast('Segmentation Title is required', 'error');
            return false;
        }

        if (!data.googleDocsUrl) {
            this.highlightError(this.googleDocsUrl);
            this.showToast('Google Docs URL is required', 'error');
            return false;
        }
        return true;
    }

    async submitAnnotation(data) {
        const response = await fetch(`${this.API_ENDPOINT}/annotation/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        });

        if (!response.ok) {
            throw new Error('Failed to add annotation');
        }

        return response.json();
    }

    async handleSubmit(event) {
        event.preventDefault();

        try {
            const data = this.getFormData();
            console.log("data ::: ", data)
            const isValid = this.validateForm(data);
            if (!isValid) {
                return;
            }
            await this.submitAnnotation(data);

            this.showToast('Annotation added successfully!', 'success');
            this.resetForm();
        } catch (error) {
            this.showToast(error.message, 'error');
            console.error('Error submitting form:', error);
        }
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
}

// Initialize the form when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new AnnotationForm();
});
