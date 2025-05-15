class UpdateMetaData {
    constructor() {
        this.elements = {
            form: document.getElementById('publishForm'),
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            searchContainers: document.querySelectorAll('.select-search-container'),
            pechaLoadingSpinner: document.getElementById('pechaLoadingSpinner'),
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            annotationAlignmentContainer: document.getElementById('annotationAlignmentContainer'),
            annotationLoadingSpinner: document.getElementById('annotationLoadingSpinner'),
            annotationAlignmentSelect: document.getElementById('annotationAlignment'),
            pechaSelect: document.getElementById('pecha'),
            publishButton: document.getElementById('publishButton'),
            toastContainer: document.getElementById('toastContainer'),
            metadataContainer: document.querySelector('.metadata-container'),
            metadataLoadingSpinner: document.getElementById('metadataLoadingSpinner')
        };

        this.isLoading = false;
        this.metadata = null
        this.initialize();
    }
    
    async initialize() {
        try {
            this.API_ENDPOINT = await getApiEndpoint();
            await this.fetchPechaOptions();
            this.initializeSearchUI();
            this.setupEventListeners();
            this.showInitialMetadataState();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize the application. Please refresh the page.', 'error');
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
        // Listen for changes on pecha selection
        this.elements.pechaSelect.addEventListener('change', () => this.handlePechaSelect());
        
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

    async fetchPechaOptions() {
        this.toggleLoadingSpinner(true);
        console.log(this.API_ENDPOINT);
        try {
            let allPechas = [];
            let currentPage = 1;
            let hasMorePages = true;
            const limit = 100; // Keep the same limit per request
            
            // Loop until we've fetched all pages
            while (hasMorePages) {
                const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                    method: 'POST',
                    headers: {
                        'accept': 'application/json',
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        "filter": {
                        },
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
            this.elements.pechaOptionsContainer.classList.add('loading');
        } else {
            this.elements.pechaLoadingSpinner.classList.remove('active');
            this.elements.pechaOptionsContainer.classList.remove('loading');
        }
    }

    populatePechaDropdown(pechas) {
        console.log("pechas", pechas)
        while (this.elements.pechaSelect.options.length > 1) {
            this.elements.pechaSelect.remove(1);
        }
        pechas.forEach(pecha => {
            const title = pecha.title[pecha.language] ?? pecha.title.bo;
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

    toggleAnnotationLoadingSpinner(isLoading) {
        if (isLoading) {
            this.elements.annotationAlignmentContainer.style.display = 'none';
            this.elements.annotationLoadingSpinner.style.display = 'flex';
        } else {
            this.elements.annotationAlignmentContainer.style.display = 'block';
            this.elements.annotationLoadingSpinner.style.display = 'none';
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

    async displayMetadata(metadata) {
        const reorderedMetadata = this.reorderMetadata(metadata);
        let metadataHTML = '';
        
        // First, create HTML for non-category items
        for (const [key, value] of Object.entries(reorderedMetadata)) {
            if (!value || key === 'category') continue;
            
            const formattedKey = key.replace(/_/g, ' ').toUpperCase();
            const formattedValue = this.formatMetadataValue(value);
            
            metadataHTML += `
                <div class="metadata-item">
                    <div class="metadata-key">${formattedKey}</div>
                    <div class="metadata-value">${formattedValue}</div>
                </div>
            `;
        }
        
        // Handle category separately if it exists
        if (reorderedMetadata.category) {
            const formattedKey = 'CATEGORY';
            const categoryId = reorderedMetadata.category;
            
            // Add a placeholder for category with loading indicator
            metadataHTML += `
                <div class="metadata-item" id="category-metadata-item">
                    <div class="metadata-key">${formattedKey}</div>
                    <div class="metadata-value">
                        <div class="category-loading">
                            <div class="loading-spinner small"></div>
                            <span>Loading category...</span>
                        </div>
                    </div>
                </div>
            `;
        }
        
        this.elements.metadataContainer.innerHTML = `
            <div class="metadata-content">
                ${metadataHTML}
            </div>
        `;
        
        // Now fetch and update the category chain if needed
        if (reorderedMetadata.category) {
            try {
                const categoryChain = await this.getCategoryChain(reorderedMetadata.category);
                const categoryItem = document.getElementById('category-metadata-item');
                if (categoryItem) {
                    categoryItem.querySelector('.metadata-value').innerHTML = categoryChain;
                }
            } catch (error) {
                console.error('Error updating category chain:', error);
                const categoryItem = document.getElementById('category-metadata-item');
                if (categoryItem) {
                    categoryItem.querySelector('.metadata-value').innerHTML = reorderedMetadata.category;
                }
            }
        }
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
    async getCategoryChain(categoryId) {
        try {
            const response = await fetch(`${this.API_ENDPOINT}/categories`, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            
            const data = await response.json();
            const chain = this.findCategoryChain(data.categories, categoryId);
            
            if (chain && chain.length > 0) {
                return chain.join(' > ');
            } else {
                return categoryId; // Return the ID if chain not found
            }
        } catch (error) {
            console.error('Error fetching category chain:', error);
            return categoryId; // Return the ID if there's an error
        }
    }
    
    findCategoryChain(data, targetId) {
        function searchInData(items, currentPath = []) {
          for (const item of items) {
            const newPath = [...currentPath, item.name['en']];
            
            // If this is our target, return the path
            if (item.id === targetId) {
              return newPath;
            }
            
            // If this item has subcategories, search in them
            if (item.subcategories && item.subcategories.length > 0) {
              const result = searchInData(item.subcategories, newPath);
              // If we found the target in the subcategories, return the result
              if (result) {
                return result;
              }
            }
          }
          
          // If we've examined all items and didn't find the target, return null
          return null;
        }
        return searchInData(data);
    }
    async handlePechaSelect() {
        const pechaId = this.elements.pechaSelect.value;
        if (!pechaId) {
            this.showInitialMetadataState();
            return;
        }

        try {
            // Show loading state for metadata
            this.showLoadingState();
            
            // Fetch metadata
            const metadata = await this.fetchMetadata(pechaId);
            this.displayMetadata(metadata);
            
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
   
    async getPublishDistination() {
        try {
            const firebaseConfigResponse = await fetch('/__/firebase/init.json', {
                headers: {
                    'Accept': 'application/json',
                    'Cache-Control': 'no-cache'
                },
                cache: 'no-store'
            });
            if (!firebaseConfigResponse.ok) {
                throw new Error("Failed to fetch Firebase init file");
            }
            const firebaseConfig = await firebaseConfigResponse.json();
            const projectId = firebaseConfig.projectId;
            if (projectId === 'pecha-backend') {
                return 'production';
            } else if (projectId === 'pecha-backend-dev') {
                return 'staging';
            }
        } catch (error) {
            console.warn("Failed to fetch Firebase init file:", error);
            return null;
        }
    }
    

    async validateFields() {
        if (!this.metadata.category) {
            this.showToast('This pecha does not have category', 'error');
            return false;
        }
        const publishTextId = this.elements.pechaSelect.value;
        if (!publishTextId) {
            this.showToast('Please select the pecha OPF', 'error');
            return false;
        }

        const annotation_id = this.elements.annotationAlignmentSelect.value;
        if (!annotation_id) {
            this.showToast('Please select the annotation alignment', 'error');
            return false;
        }
        const publishDestination = await this.getPublishDistination();
        if(!publishDestination) {
            this.showToast('Please select the publish destination', 'error');
            return false;
        }

        return { publishTextId, publishDestination, reserialize: true, annotation_id };
    }

    async handlePublish() {
        const validatedData = await this.validateFields();
        if (!validatedData) return;

        this.setLoading(true);

        try {
            const { publishTextId, publishDestination, reserialize, annotation_id } = validatedData;
            console.log("validatedData",validatedData)
            const response = await fetch(`${this.API_ENDPOINT}/pecha/${publishTextId}/publish`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ destination: publishDestination, reserialize, annotation_id })
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