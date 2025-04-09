class PechaList {
    constructor() {
        // Initialize elements
        this.elements = {
            searchInput: document.getElementById('search-input'),
            searchButton: document.getElementById('search-button'),
            advancedFilterButton: document.getElementById('advanced-filter-button'),
            filterModal: document.getElementById('filter-modal'),
            closeModal: document.querySelector('.close-modal'),
            applyFiltersButton: document.getElementById('apply-filters'),
            resetFiltersButton: document.getElementById('reset-filters'),
            sortSelect: document.getElementById('sort-select'),
            totalResults: document.getElementById('total-results'),
            resultsGrid: document.getElementById('results-grid'),
            prevPageButton: document.getElementById('prev-page'),
            nextPageButton: document.getElementById('next-page'),
            pageInfo: document.getElementById('page-info'),
            categorySelect: document.getElementById('category-select'),
            toastContainer: document.getElementById('toastContainer')
        };

        // Initialize state
        this.state = {
            currentPage: 1,
            totalPages: 1,
            itemsPerPage: 12,
            allPechas: [],
            filteredPechas: [],
            currentSort: 'relevance',
            currentFilters: {
                search: '',
                relationships: [],
                languages: [],
                category: ''
            },
            isLoading: true,
            hasMoreData: true,
            isLoadingMore: false,
            observer: null,
            visibleItems: new Set(),
            itemHeight: 250, // Estimated height of each item in pixels
            bufferItems: 5, // Number of items to render above and below visible area
            totalItems: 0,
            apiPage: 1,
            apiLimit: 20
        };

        // Initialize the application
        this.init();
    }

    // Toast notification methods
    showToast(message, type = 'info') {
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
    
    // Data methods
    async fetchPechas(page = 1, limit = 20) {
        console.log(`Fetching page ${page} with limit ${limit} from ${this.API_ENDPOINT}`);
        this.state.isLoadingMore = true;
        
        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    "filter": this.buildApiFilter(),
                    "page": page,
                    "limit": limit
                })
            });
            
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            
            const data = await response.json();
            console.log("API response:", data);
            
            // Update pagination info from API
            if (data.pagination) {
                this.state.totalItems = data.pagination.total;
                this.state.hasMoreData = data.pagination.page * data.pagination.limit < data.pagination.total;
            }
            
            this.state.isLoading = false;
            this.state.isLoadingMore = false;
            
            return data.metadata || [];
        } catch (error) {
            console.error('Error fetching pechas:', error);
            this.showToast('Error loading pechas. Please try again.', 'error');
            this.state.isLoading = false;
            this.state.isLoadingMore = false;
            return [];
        }
    }
    
    // Build API filter based on current UI filters
    buildApiFilter() {
        let filter = {};

        // Add search term
        if (this.state.currentFilters.search) {
            filter.search = this.state.currentFilters.search;
        }
        
        // For advanced filters only (not search)
        
        // Add relationship filters
        if (this.state.currentFilters.relationships.length > 0) {
            filter.relationships = {};
            
            this.state.currentFilters.relationships.forEach(rel => {
                filter = {
                    "field": rel,
                    "operator": "!=",
                    "value": null
                };
            });
        }
        
        // Add language filters
        if (this.state.currentFilters.languages.length > 0) {
            filter.languages = {
                "field": "language",
                "operator": "in",
                "value": this.state.currentFilters.languages
            };
        }
        
        // Add category filter
        if (this.state.currentFilters.category) {
            filter.category = {
                "field": "category",
                "operator": "==",
                "value": this.state.currentFilters.category
            };
        }
        
        return filter;
    }

    async fetchCategories() {
        try {
            const response = await fetch(`${this.API_ENDPOINT}/categories/`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const categories = await response.json();
            // this.populateCategories(categories);
        } catch (error) {
            console.error('Error fetching categories:', error);
            this.showToast('Error loading categories. Please try again.', 'error');
        }
    }

    populateCategories(data) {
        // Clear existing options except the default one
        const select = this.elements.categorySelect;
        while (select.options.length > 1) {
            select.remove(1);
        }

        // Add new options
        data.forEach(category => {
            const option = document.createElement('option');
            option.value = category.id;
            option.textContent = category.name;
            select.appendChild(option);
        });
    }

    // Filtering and sorting methods
    async filterPechas() {
        // Reset pagination
        this.state.currentPage = 1;
        this.state.apiPage = 1;
        this.state.isLoading = true;
        
        // Show skeleton loading
        this.displaySkeletons();
        
        // Check if we need to do an API request for advanced filters
        const hasAdvancedFilters = 
            this.state.currentFilters.relationships.length > 0 || 
            this.state.currentFilters.languages.length > 0 || 
            this.state.currentFilters.category !== '';
        
        let filteredData = [];
        
        if (hasAdvancedFilters) {
            console.log("Applying advanced filters");
            // Use API for advanced filtering
            filteredData = await this.fetchPechas(this.state.apiPage, this.state.apiLimit);
            this.state.allPechas = filteredData; // Update all pechas with the filtered results
        } else {
            console.log("Applying basic filters");
            console.log(this.state.allPechas)
            // For basic search (ID and title), filter locally from allPechas
            filteredData = this.state.allPechas.filter(pecha => {
                // If no search term, include all
                if (!this.state.currentFilters.search) {
                    return true;
                }
                
                const searchTerm = this.state.currentFilters.search.toLowerCase();
                
                // Check ID
                if (pecha.id && pecha.id.toLowerCase().includes(searchTerm)) {
                    return true;
                }
                
                // Check title in any available language
                if (pecha.title) {
                    for (const lang in pecha.title) {
                        if (pecha.title[lang] && pecha.title[lang].toLowerCase().includes(searchTerm)) {
                            return true;
                        }
                    }
                }
                
                return false;
            });
        }
        
        console.log("Filtered data:", filteredData);
        this.state.filteredPechas = filteredData;
        
        // Apply sorting
        this.sortPechas();
        
        // Update UI
        this.updateTotalResults();
        this.updatePagination();
        this.displayPechas();
        
        // Set up intersection observer for lazy loading
        // this.setupLazyLoading();
    }

    sortPechas() {
        const sort = this.state.currentSort;
        
        this.state.filteredPechas.sort((a, b) => {
            switch (sort) {
                case 'title-asc':
                    return this.getTitle(a).localeCompare(this.getTitle(b));
                case 'title-desc':
                    return this.getTitle(b).localeCompare(this.getTitle(a));
                case 'id-asc':
                    return a.id.localeCompare(b.id);
                case 'id-desc':
                    return b.id.localeCompare(a.id);
                default: // relevance - keep as is
                    return 0;
            }
        });
    }
    
    // Helper to get title in any available language
    getTitle(pecha) {
        if (!pecha.title) return '';
        
        // Try to get English title first
        if (pecha.title.en) return pecha.title.en;
        
        // Otherwise get the first available title
        const lang = Object.keys(pecha.title)[0];
        return pecha.title[lang] || '';
    }

    // UI methods
    displayPechas() {
        const resultsGrid = this.elements.resultsGrid;
        
        // Clear existing results
        resultsGrid.innerHTML = '';
        
        // If no results, show message
        if (this.state.filteredPechas.length === 0 && !this.state.isLoading) {
            resultsGrid.innerHTML = '<div class="no-results">No pechas found matching your criteria.</div>';
            return;
        }
        
        // Create a direct grid container without nesting
        const gridContainer = document.createElement('div');
        gridContainer.className = 'results-grid'; // Use the original grid class
        
        // Add pechas to the grid
        this.state.filteredPechas.forEach((pecha, index) => {
            const card = this.createPechaCard(pecha, index);
            gridContainer.appendChild(card);
        });
        
        // Add lazy loading trigger if there might be more data
        if (this.state.hasMoreData) {
            const loadTrigger = document.createElement('div');
            loadTrigger.className = 'lazy-load-trigger';
            loadTrigger.id = 'lazy-load-trigger';
            loadTrigger.style.gridColumn = '1 / -1'; // Make it span all columns in the grid
            
            if (this.state.isLoadingMore) {
                const spinner = document.createElement('div');
                spinner.className = 'loading-spinner';
                loadTrigger.appendChild(spinner);
            }
            
            gridContainer.appendChild(loadTrigger);
        }
        
        resultsGrid.appendChild(gridContainer);
    }
    
    createPechaCard(pecha, index) {
        const card = document.createElement('div');
        card.className = 'pecha-card';
        card.dataset.index = index;
        
        // Lazy load the card when it comes into view
        card.setAttribute('loading', 'lazy');
        
        const content = document.createElement('div');
        content.className = 'pecha-card-content';
        
        // ID
        const id = document.createElement('h3');
        id.className = 'pecha-id';
        id.textContent = `ID: ${pecha.id}`;
        content.appendChild(id);
        
        // Title
        const title = document.createElement('h2');
        title.className = 'pecha-title';
        
        // Get title in any available language
        title.textContent = this.getTitle(pecha);
        content.appendChild(title);
        
        // Author
        if (pecha.author) {
            const author = document.createElement('p');
            author.className = 'pecha-author';
            
            // Get author in any available language
            const authorLang = pecha.author.en ? 'en' : Object.keys(pecha.author)[0];
            author.textContent = `Author: ${authorLang ? pecha.author[authorLang] : 'Unknown'}`;
            content.appendChild(author);
        }
        
        // Relationships
        const relationships = document.createElement('div');
        relationships.className = 'pecha-relationships';
        
        const relItems = [];
        if (pecha.version_of) relItems.push(`version of ${pecha.version_of}`);
        if (pecha.commentary_of) relItems.push(`commentary of ${pecha.commentary_of}`);
        if (pecha.translation_of) relItems.push(`translation of ${pecha.translation_of}`);
        
        if (relItems.length > 0) {
            relationships.textContent = `Relationships: [${relItems.join(', ')}]`;
            content.appendChild(relationships);
        }
        
        card.appendChild(content);
        return card;
    }
    
    displaySkeletons() {
        const resultsGrid = this.elements.resultsGrid;
        resultsGrid.innerHTML = '';
        
        // Create a direct grid container
        const gridContainer = document.createElement('div');
        gridContainer.className = 'results-grid';
        
        // Create 12 skeleton cards (one page worth)
        for (let i = 0; i < this.state.itemsPerPage; i++) {
            const skeletonCard = document.createElement('div');
            skeletonCard.className = 'pecha-card';
            
            const content = document.createElement('div');
            content.className = 'pecha-card-content';
            
            // Skeleton ID
            const id = document.createElement('div');
            id.className = 'skeleton skeleton-id';
            content.appendChild(id);
            
            // Skeleton Title
            const title = document.createElement('div');
            title.className = 'skeleton skeleton-title';
            content.appendChild(title);
            
            // Skeleton Author
            const author = document.createElement('div');
            author.className = 'skeleton skeleton-author';
            content.appendChild(author);
            
            // Skeleton Relationships
            const relationships = document.createElement('div');
            relationships.className = 'skeleton skeleton-relationships';
            content.appendChild(relationships);
            
            skeletonCard.appendChild(content);
            gridContainer.appendChild(skeletonCard);
        }
        
        resultsGrid.appendChild(gridContainer);
    }
    
    setupLazyLoading() {
        // Disconnect previous observer if it exists
        if (this.state.observer) {
            this.state.observer.disconnect();
        }
        
        // Create new intersection observer for lazy loading
        this.state.observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting && !this.state.isLoadingMore && this.state.hasMoreData) {
                    this.loadMorePechas();
                }
            });
        }, { rootMargin: '100px' });
        
        // Observe the lazy load trigger element
        const trigger = document.getElementById('lazy-load-trigger');
        if (trigger) {
            this.state.observer.observe(trigger);
        }
    }
    
    async loadMorePechas() {
        if (this.state.isLoadingMore || !this.state.hasMoreData) return;
        
        // Update loading state
        this.state.isLoadingMore = true;
        this.state.apiPage++;
        
        // Update the loading trigger to show spinner
        const trigger = document.getElementById('lazy-load-trigger');
        if (trigger) {
            trigger.innerHTML = '<div class="loading-spinner"></div>';
        }
        
        // Fetch more data
        const newPechas = await this.fetchPechas(this.state.apiPage, this.state.apiLimit);
        
        // Add new pechas to the list
        this.state.allPechas = [...this.state.allPechas, ...newPechas];
        this.state.filteredPechas = [...this.state.filteredPechas, ...newPechas];
        
        // Apply sorting to maintain order
        this.sortPechas();
        
        // Update UI
        this.updateTotalResults();
        this.displayPechas();
        
        // Set up intersection observer for the new lazy loading trigger
        this.setupLazyLoading();
    }
    
    updateTotalResults() {
        this.elements.totalResults.textContent = `Total: ${this.state.totalItems}`;
    }

    updatePagination() {
        this.state.totalPages = Math.ceil(this.state.totalItems / this.state.itemsPerPage);
        this.elements.pageInfo.textContent = `Page ${this.state.currentPage} of ${this.state.totalPages || 1}`;
        this.elements.prevPageButton.disabled = this.state.currentPage <= 1;
        this.elements.nextPageButton.disabled = this.state.currentPage >= this.state.totalPages;
    }

    // Event handlers
    handleSearch() {
        // Update search term in filters
        this.state.currentFilters.search = this.elements.searchInput.value.trim();
        
        // Use local filtering for basic search
        this.filterPechas();
    }

    handleAdvancedFilter() {
        this.elements.filterModal.style.display = 'block';
    }

    handleCloseModal() {
        this.elements.filterModal.style.display = 'none';
    }

    handleApplyFilters() {
        // Get relationship filters
        const relationshipCheckboxes = document.querySelectorAll('input[name="relationship"]:checked');
        this.state.currentFilters.relationships = Array.from(relationshipCheckboxes).map(cb => cb.value);
        
        // Get language filters
        const languageCheckboxes = document.querySelectorAll('input[name="language"]:checked');
        this.state.currentFilters.languages = Array.from(languageCheckboxes).map(cb => cb.value);
        
        // Get category filter
        this.state.currentFilters.category = this.elements.categorySelect.value;
        console.log("Applied filters:", this.state.currentFilters);
        
        // Apply filters - this will use API for advanced filters
        this.filterPechas();
        
        // Close modal
        this.elements.filterModal.style.display = 'none';
    }

    handleResetFilters() {
        // Uncheck all checkboxes
        document.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
        
        // Reset category select
        this.elements.categorySelect.value = '';
        
        // Reset filters object
        this.state.currentFilters = {
            search: this.elements.searchInput.value.trim(), // Keep the search term
            relationships: [],
            languages: [],
            category: ''
        };
        
        // Apply reset filters
        this.filterPechas();
    }

    handleSortChange() {
        this.state.currentSort = this.elements.sortSelect.value;
        this.sortPechas();
        this.displayPechas();
    }

    handlePrevPage() {
        if (this.state.currentPage > 1) {
            this.state.currentPage--;
            this.updatePagination();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }

    handleNextPage() {
        if (this.state.currentPage < this.state.totalPages) {
            this.state.currentPage++;
            this.updatePagination();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }

    // Setup event listeners
    setupEventListeners() {
        // Search button click
        this.elements.searchButton.addEventListener('click', () => this.handleSearch());
        
        // Search input enter key
        this.elements.searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.handleSearch();
            }
        });
        
        // Advanced filter button
        this.elements.advancedFilterButton.addEventListener('click', () => this.handleAdvancedFilter());
        
        // Close modal button
        this.elements.closeModal.addEventListener('click', () => this.handleCloseModal());
        
        // Close modal when clicking outside
        window.addEventListener('click', (e) => {
            if (e.target === this.elements.filterModal) {
                this.handleCloseModal();
            }
        });
        
        // Apply filters button
        this.elements.applyFiltersButton.addEventListener('click', () => this.handleApplyFilters());
        
        // Reset filters button
        this.elements.resetFiltersButton.addEventListener('click', () => this.handleResetFilters());
        
        // Sort select change
        this.elements.sortSelect.addEventListener('change', () => this.handleSortChange());
        
        // Previous page button
        this.elements.prevPageButton.addEventListener('click', () => this.handlePrevPage());
        
        // Next page button
        this.elements.nextPageButton.addEventListener('click', () => this.handleNextPage());
    }
    
    // Initialize the application
    async init() {
        // Show skeleton loading
        this.displaySkeletons();
        
        // Fetch pechas data
        await this.loadConfig();
        this.state.allPechas = await this.fetchPechas(this.state.apiPage, this.state.apiLimit);
        this.state.filteredPechas = [...this.state.allPechas];
        
        // Populate categories
        this.fetchCategories();
        
        // Set up event listeners
        this.setupEventListeners();
        
        // Set up pagination
        this.updateTotalResults();
        this.updatePagination();
        
        // Display initial results
        this.displayPechas();
        
        // Set up lazy loading
        this.setupLazyLoading();
    }
}

// Initialize the application when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new PechaList();
});
