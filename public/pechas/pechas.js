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
            endPageIndicator: document.getElementById('end-page-indicator'),
            categorySelect: document.getElementById('category-select'),
            toastContainer: document.getElementById('toastContainer')
        };

        // Initialize state
        this.state = {
            allPechas: [],
            filteredPechas: [],
            currentSort: 'relevance',
            currentFilters: {
                search: '',
                types: [],
                languages: [],
                category: ''
            },
            isLoading: true,
            hasMoreData: true,
            isLoadingMore: false,
            observer: null,
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

    // Data methods
    async fetchPechas(page = 1, limit = 20) {
        console.log(`Fetching page ${page} with limit ${limit} from ${this.API_ENDPOINT}`);
        this.state.isLoadingMore = true;

        try {
            // Build the request body based on whether we have advanced filters
            const hasAdvancedFilters =
                this.state.currentFilters.types.length > 0 ||
                this.state.currentFilters.languages.length > 0 ||
                this.state.currentFilters.category !== '';

            let requestBody = {};

            // Add filter if we have advanced filters
            if (hasAdvancedFilters) {
                const filter = this.buildApiFilter();
                requestBody.filter = filter;
            }
            const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    ...requestBody,
                    page: page,
                    limit: limit
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

            // Check if the data has the expected structure
            if (data.metadata && Array.isArray(data.metadata)) {
                return data.metadata;
            } else if (data.results && Array.isArray(data.results)) {
                return data.results;
            } else {
                console.warn("Unexpected API response structure:", data);
                return [];
            }
        } catch (error) {
            console.error("Error fetching pechas:", error);
            this.showToast(`Error loading data: ${error.message}`, 'error');
            return [];
        } finally {
            this.state.isLoading = false;
            this.state.isLoadingMore = false;
        }
    }

    // Build API filter based on current UI filters
    buildApiFilter() {
        console.log("Building API filter");
        if (this.state.currentFilters.types.length > 0) {
            const typeFilter = {
                "field": "type",
                "operator": "==",
                "value": this.state.currentFilters.types[0]
            };
            console.log("Built type filter:", typeFilter);
            return typeFilter;
        }
        if (this.state.currentFilters.languages.length > 0) {
            const languageFilter = {
                "field": "language",
                "operator": "==",
                "value": this.state.currentFilters.languages[0]
            };
            console.log("Built language filter:", languageFilter);
            return languageFilter;
        }
        if (this.state.currentFilters.category) {
            const categoryFilter = {
                "field": "category",
                "operator": "==",
                "value": this.state.currentFilters.category
            };
            console.log("Built category filter:", categoryFilter);
            return categoryFilter;
        }

        // No filters
        console.log("No filters applied");
        return {};
    }

    async fetchCategories() {
        try {
            const response = await fetch(`${this.API_ENDPOINT}/categories/`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const categories = await response.json();
            console.log("Categories:", categories);
            const innermostIds = this.extractInnermostIds(categories);
            this.populateCategories(innermostIds);
        } catch (error) {
            console.error('Error fetching categories:', error);
            this.showToast('Error loading categories. Please try again.', 'error');
        }
    }

    extractInnermostIds(data) {
        const innermostIds = [];

        function traverse(category) {
            // If this category has no subcategories or empty subcategories array, it's innermost
            if (!category?.subcategories || category.subcategories.length === 0) {
                innermostIds.push(category.id);
            } else {
                // Otherwise, traverse each subcategory
                category?.subcategories?.forEach(subcategory => traverse(subcategory));
            }
        }

        // Start traversal with each top-level category
        data.categories?.forEach(category => traverse(category));

        return innermostIds;
    }

    populateCategories(categories) {
        // Clear existing options except the default one
        const select = this.elements.categorySelect;
        while (select.options.length > 1) {
            select.remove(1);
        }

        // Add leaf categories to the dropdown
        categories.forEach(category => {
            const option = document.createElement('option');
            option.value = category;
            option.textContent = category;

            select.appendChild(option);
        });
    }

    // Filtering and sorting methods
    async filterPechas() {
        console.log("Filtering pechas...", this.state.currentFilters)
        // Reset state for new filtering
        this.state.isLoading = true;
        this.state.apiPage = 1;
        this.state.hasMoreData = true;

        // Show skeleton loading
        this.displaySkeletons();

        // Check if we need to do an API request for advanced filters
        const hasAdvancedFilters =
            this.state.currentFilters.types.length > 0 ||
            this.state.currentFilters.languages.length > 0 ||
            this.state.currentFilters.category !== '';

        let filteredData = [];

        if (hasAdvancedFilters) {
            console.log("Applying advanced filters via API");
            // Use API for advanced filtering with the proper filter format
            filteredData = await this.fetchPechas(this.state.apiPage, this.state.apiLimit);
            this.state.allPechas = filteredData; // Update all pechas with the filtered results
            this.state.filteredPechas = filteredData;
        } else {
            console.log("Applying basic search filter locally");

            // For basic search (ID and title), filter locally from allPechas
            if (this.state.currentFilters.search) {
                // If there's a search term, filter the existing data
                filteredData = this.state.allPechas.filter(pecha => {
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

                this.state.filteredPechas = filteredData;
            } else {
                // If no search term, use all pechas
                this.state.filteredPechas = [...this.state.allPechas];
            }
        }

        console.log("Filtered data:", this.state.filteredPechas);

        // Apply sorting
        this.sortPechas();

        // Update UI
        this.updateTotalResults();
        this.displayPechas();

        // Set up intersection observer for lazy loading
        this.setupLazyLoading();
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

        // If no results and not loading, show message
        if (this.state.filteredPechas.length === 0 && !this.state.isLoading) {
            resultsGrid.innerHTML = '<div class="no-results">No pechas found matching your criteria.</div>';
            this.updateEndPageIndicator(false, 'No results found');
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

        resultsGrid.appendChild(gridContainer);

        // Update the end page indicator based on whether there's more data
        this.updateEndPageIndicator(this.state.hasMoreData);

        // Set up intersection observer for lazy loading
        this.setupLazyLoading();
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
        for (let i = 0; i < this.state.apiLimit; i++) {
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

        // Observe the end page indicator element
        if (this.elements.endPageIndicator) {
            this.state.observer.observe(this.elements.endPageIndicator);
        }
    }

    async loadMorePechas() {
        if (this.state.isLoadingMore || !this.state.hasMoreData) return;

        // Update loading state
        this.state.isLoadingMore = true;
        this.state.apiPage++;

        // Update the loading indicator
        this.updateEndPageIndicator(true, 'Loading more results...');

        // Fetch more data
        const newPechas = await this.fetchPechas(this.state.apiPage, this.state.apiLimit);

        // Add new pechas to the list
        this.state.allPechas = [...this.state.allPechas, ...newPechas];
        this.state.filteredPechas = [...this.state.filteredPechas, ...newPechas];

        // Apply sorting to maintain order
        this.sortPechas();

        // Update UI
        this.updateTotalResults();

        // Append new pechas to the grid without clearing existing ones
        const gridContainer = document.querySelector('.results-grid');
        if (gridContainer) {
            newPechas.forEach((pecha, index) => {
                const card = this.createPechaCard(pecha, this.state.filteredPechas.length - newPechas.length + index);
                gridContainer.appendChild(card);
            });
        }

        // Update loading state
        this.state.isLoadingMore = false;

        // Update end page indicator
        this.updateEndPageIndicator(this.state.hasMoreData);
    }

    updateEndPageIndicator(hasMoreData, message = null) {
        const indicator = this.elements.endPageIndicator;
        if (!indicator) return;

        const loadingText = indicator.querySelector('.loading-text');

        if (hasMoreData) {
            // Still has more data to load
            indicator.classList.remove('complete');
            if (loadingText && message) {
                loadingText.textContent = message;
            } else if (loadingText) {
                loadingText.textContent = 'Loading more results...';
            }
        } else {
            // No more data, show end of results
            indicator.classList.add('complete');
            if (loadingText) {
                loadingText.textContent = message || 'End of results';
            }
        }
    }

    updateTotalResults() {
        // Show the count of currently filtered/displayed results
        const displayCount = this.state.filteredPechas.length;
        this.elements.totalResults.textContent = `Total: ${displayCount}`;
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
        // Reset all current filters
        this.state.currentFilters.types = [];
        this.state.currentFilters.languages = [];
        this.state.currentFilters.category = '';

        // Get the selected radio button filter
        const selectedRadio = document.querySelector('input[name="single-filter"]:checked');
        if (selectedRadio) {
            const filterType = selectedRadio.getAttribute('data-type');
            const filterValue = selectedRadio.value;

            if (filterType === 'type') {
                this.state.currentFilters.types = [filterValue];
            } else if (filterType === 'language') {
                this.state.currentFilters.languages = [filterValue];
            }
        }

        // Get category filter (this takes precedence over radio buttons)
        const categoryValue = this.elements.categorySelect.value;
        if (categoryValue) {
            this.state.currentFilters.category = categoryValue;
            // If category is selected, clear radio button selection
            const checkedRadio = document.querySelector('input[name="single-filter"]:checked');
            if (checkedRadio) {
                checkedRadio.checked = false;
            }
        }

        console.log("Applied filters:", this.state.currentFilters);

        // Apply filters - this will use API for advanced filters
        this.filterPechas();

        // Close modal
        this.elements.filterModal.style.display = 'none';
    }

    async handleResetFilters() {
        // Uncheck all radio buttons
        document.querySelectorAll('input[name="single-filter"]').forEach(radio => radio.checked = false);

        // Reset category select
        this.elements.categorySelect.value = '';

        // Reset filters object but keep search term
        const currentSearchTerm = this.elements.searchInput.value.trim();
        this.state.currentFilters = {
            search: currentSearchTerm,
            types: [],
            languages: [],
            category: ''
        };

        // Close modal
        this.elements.filterModal.style.display = 'none';

        // Reset pagination state
        this.state.apiPage = 1;
        this.state.hasMoreData = true;
        this.state.isLoading = true;

        // Show loading state
        this.displaySkeletons();

        console.log("Resetting to initial state - calling API");

        try {
            // Call API to get initial data (like page load)
            const initialPechas = await this.fetchPechas(this.state.apiPage, this.state.apiLimit);
            console.log("Reset: Initial pechas loaded:", initialPechas.length);

            // Update state with fresh data from API
            this.state.allPechas = initialPechas;

            // If there's a search term, apply it locally to the fresh data
            if (currentSearchTerm) {
                console.log("Reset: Applying local search for:", currentSearchTerm);

                // Filter locally from the fresh API data
                this.state.filteredPechas = this.state.allPechas.filter(pecha => {
                    const searchTerm = currentSearchTerm.toLowerCase();

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
            } else {
                // No search term, show all fresh data
                console.log("Reset: Showing all fresh data from API");
                this.state.filteredPechas = [...this.state.allPechas];
            }

            // Apply current sorting
            this.sortPechas();

            // Update UI
            this.updateTotalResults();
            this.displayPechas();

            // Set up lazy loading for the reset state
            this.setupLazyLoading();

        } catch (error) {
            console.error("Error resetting to initial state:", error);
            this.showToast(`Error resetting: ${error.message}`, 'error');
            this.state.isLoading = false;
            this.displayPechas(); // This will show the no results message
        }

    }

    handleSortChange() {
        this.state.currentSort = this.elements.sortSelect.value;
        this.sortPechas();
        this.displayPechas();
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

        // Add event listeners for mutual exclusion between radio buttons and category select
        this.setupFilterExclusionListeners();
    }

    setupFilterExclusionListeners() {
        // When a radio button is selected, clear the category select
        document.addEventListener('change', (e) => {
            if (e.target.name === 'single-filter' && e.target.checked) {
                this.elements.categorySelect.value = '';
            }
        });

        // When category is selected, clear any radio button selection
        this.elements.categorySelect.addEventListener('change', (e) => {
            if (e.target.value) {
                const checkedRadio = document.querySelector('input[name="single-filter"]:checked');
                if (checkedRadio) {
                    checkedRadio.checked = false;
                }
            }
        });
    }

    // Initialize the application
    async init() {
        // Show skeleton loading
        this.displaySkeletons();

        try {
            // Fetch pechas data
            this.API_ENDPOINT = await getApiEndpoint();
            console.log("API endpoint:", this.API_ENDPOINT);

            const initialPechas = await this.fetchPechas(this.state.apiPage, this.state.apiLimit);
            console.log("Initial pechas loaded:", initialPechas.length);

            if (initialPechas.length === 0) {
                this.showToast("No pechas found. The API may be unavailable or returned no data.", "info");
            }

            this.state.allPechas = initialPechas;
            this.state.filteredPechas = [...initialPechas];

            // Populate categories
            this.fetchCategories();

            // Update total results
            this.updateTotalResults();

            // Display initial results
            this.displayPechas();

            // Set up lazy loading
            this.setupLazyLoading();
        } catch (error) {
            console.error("Error initializing pecha list:", error);
            this.showToast(`Error initializing: ${error.message}`, 'error');
            this.state.isLoading = false;
            this.displayPechas(); // This will show the no results message
        }

        // Set up event listeners
        this.setupEventListeners();
    }
}

// Initialize the application when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new PechaList();
});
