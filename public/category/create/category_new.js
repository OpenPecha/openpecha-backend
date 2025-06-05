// Category Node Data Structure
class CategoryNode {
    constructor(id, titleEn, titleBo, titleLzh, descEn = '', descBo = '', descLzh = '', shortDescEn = '', shortdescBo = '', shortDescLzh = '') {
        this.id = id;
        this.titleEn = titleEn;
        this.titleBo = titleBo;
        this.titleLzh = titleLzh;
        this.descEn = descEn;
        this.descBo = descBo;
        this.descLzh = descLzh;
        this.shortDescEn = shortDescEn;
        this.shortdescBo = shortdescBo;
        this.shortDescLzh = shortDescLzh;
        this.children = [];
        this.parent = null;
    }

    addChild(node) {
        node.parent = this;
        this.children.push(node);
    }
}

// Main Application Class
class CategoryManager {
    constructor() {
        this.API_ENDPOINT = '';
        this.root = null;
        this.selectedRoot = null;
        this.selectedNode = null;
        this.categories = [];
        this.options = [];
        this.isLoading = false;
        this.isMetadataCollapsed = false;
        this.isRelatedPechasCollapsed = false;
        this.selectedRelatedPechas = []; // Array to track selected related pecha IDs

        this.elements = {};
        this.init();
    }

    async init() {
        try {
            console.log('CategoryManager: Initializing...');
            await this.setupApiEndpoint();
            this.setupElements();
            this.setupEventListeners();
            this.initializeSearchUI();
            await this.fetchPechaOptions();
            await this.fetchCategories();
            this.showInitialMetadataState();
            this.showInitialRelatedPechasState();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }

    async setupApiEndpoint() {
        try {
            this.API_ENDPOINT = await getApiEndpoint();
        } catch (error) {
            console.error('Failed to get API endpoint:', error);
            throw error;
        }
    }

    setupElements() {
        console.log('Setting up DOM elements...');

        // Safely get elements with null checks
        this.elements = {
            // Category elements
            categorySelector: document.getElementById('categorySelector'),
            categoryTree: document.getElementById('categoryTree'),
            addCategoryBtn: document.getElementById('addCategory'),
            selectedCategoryLabel: document.querySelector('.selected-category'),

            // Pecha elements
            pechaSelect: document.getElementById('pechaOptions'),
            pechaLoadingSpinner: document.getElementById('pechaLoadingSpinner'),

            // Metadata elements
            metadataContent: document.querySelector('.metadata-content'),
            metadataHeader: document.querySelector('.metadata-header'),
            toggleBtn: document.querySelector('.toggle-btn'),

            // Related Pechas elements
            relatedPechasContent: document.querySelector('.related-pechas-content'),
            relatedPechasHeader: document.querySelector('.related-pechas-header'),
            relatedPechasToggleBtn: document.querySelector('.related-pechas-header .toggle-btn'),

            // Form elements
            destinationRadios: document.querySelectorAll('input[name="destination"]'),

            // Toast
            toastContainer: document.getElementById('toastContainer'),

            // Search overlays
            searchContainers: document.querySelectorAll('.select-search-container')
        };

        // Log warnings for missing critical elements
        const criticalElements = ['categorySelector', 'categoryTree', 'addCategoryBtn', 'pechaSelect', 'toastContainer'];
        criticalElements.forEach(key => {
            if (!this.elements[key]) {
                console.warn(`Critical element not found: ${key}`);
            }
        });
    }

    setupEventListeners() {
        // Pecha selection
        if (this.elements.pechaSelect) {
            this.elements.pechaSelect.addEventListener('change', () => {
                this.handlePechaSelect();
            });
        }

        // Category assignment
        if (this.elements.addCategoryBtn) {
            this.elements.addCategoryBtn.addEventListener('click', () => {
                this.assignCategory();
            });
        }

        // Metadata toggle
        if (this.elements.metadataHeader) {
            this.elements.metadataHeader.addEventListener('click', () => {
                this.toggleMetadata();
            });
        }

        // Related Pechas toggle
        if (this.elements.relatedPechasHeader) {
            this.elements.relatedPechasHeader.addEventListener('click', () => {
                this.toggleRelatedPechas();
            });
        }
    }

    initializeSearchUI() {
        if (!this.elements.searchContainers || this.elements.searchContainers.length === 0) {
            console.warn('No search containers found');
            return;
        }

        this.elements.searchContainers.forEach(container => {
            const select = container.querySelector('select');
            const searchOverlay = container.querySelector('.search-overlay');
            const searchInput = container.querySelector('.search-input');
            const searchResults = container.querySelector('.search-results');

            if (!select || !searchOverlay || !searchInput || !searchResults) return;

            // Toggle search overlay
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

            // Filter results
            searchInput.addEventListener('input', () => {
                this.populateSearchResults(select, searchResults, searchInput.value);
            });

            // Handle selection
            searchResults.addEventListener('click', (e) => {
                if (e.target.classList.contains('search-item')) {
                    const value = e.target.dataset.value;
                    select.value = value;
                    select.dispatchEvent(new Event('change', { bubbles: true }));
                    searchOverlay.classList.remove('active');
                }
            });

            // Keyboard navigation
            searchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') {
                    searchOverlay.classList.remove('active');
                } else if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    this.navigateSearchResults(searchResults, 'next');
                } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    this.navigateSearchResults(searchResults, 'prev');
                } else if (e.key === 'Enter') {
                    e.preventDefault();
                    const activeItem = searchResults.querySelector('.search-item.active');
                    if (activeItem) activeItem.click();
                }
            });
        });
    }

    populateSearchResults(select, resultsContainer, searchTerm) {
        const options = Array.from(select.options);
        const filteredOptions = options.filter(option =>
            option.text.toLowerCase().includes(searchTerm.trim().toLowerCase())
        );

        resultsContainer.innerHTML = '';
        filteredOptions.forEach(option => {
            if (option.value) {
                const item = document.createElement('div');
                item.className = 'search-item';
                item.textContent = option.text;
                item.dataset.value = option.value;
                resultsContainer.appendChild(item);
            }
        });
    }

    navigateSearchResults(resultsContainer, direction) {
        const items = resultsContainer.querySelectorAll('.search-item');
        const activeItem = resultsContainer.querySelector('.search-item.active');
        let nextIndex;

        if (!activeItem) {
            nextIndex = direction === 'next' ? 0 : items.length - 1;
        } else {
            const currentIndex = Array.from(items).indexOf(activeItem);
            if (direction === 'next') {
                nextIndex = currentIndex + 1 >= items.length ? 0 : currentIndex + 1;
            } else {
                nextIndex = currentIndex - 1 < 0 ? items.length - 1 : currentIndex - 1;
            }
            activeItem.classList.remove('active');
        }

        if (items[nextIndex]) {
            items[nextIndex].classList.add('active');
            items[nextIndex].scrollIntoView({ block: 'nearest' });
        }
    }

    async fetchCategories() {
        try {
            console.log('Fetching categories...');
            const response = await fetch(`${this.API_ENDPOINT}/categories`, {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });

            if (!response.ok) throw new Error('Network response was not ok');

            const data = await response.json();
            this.categories = data;
            this.options = this.extractCategoryNames(data);
            this.setupCategorySelector();

        } catch (error) {
            console.error('Error fetching categories:', error);
            this.showToast('Failed to load categories. Please try again.', 'error');
        }
    }

    extractCategoryNames(data) {
        if (!data || !data.categories) {
            console.warn('Invalid categories data structure');
            return [];
        }
        return data.categories.map(category => ({
            id: category.id,
            name: `${category.name["bo"]} (${category.name["en"]}) ${category?.name["lzh"] ? "(" + category?.name["lzh"] + ")" : ""}`
        }));
    }

    setupCategorySelector() {
        if (!this.elements.categorySelector) {
            console.warn('Category selector element not found');
            return;
        }

        // Add loading state
        this.elements.categorySelector.classList.add('loading');

        // Create select element if it doesn't exist
        let select = this.elements.categorySelector.querySelector('select');
        if (!select) {
            select = document.createElement('select');
            select.id = 'categorySelect';
            select.className = 'category-dropdown';
            this.elements.categorySelector.appendChild(select);
        }

        // Clear and populate options
        select.innerHTML = '<option value="">Select a category...</option>';

        if (this.options && this.options.length > 0) {
            this.options.forEach(option => {
                const optionElement = document.createElement('option');
                optionElement.value = option.id;
                optionElement.textContent = option.name;
                select.appendChild(optionElement);
            });
        } else {
            const noOptionsElement = document.createElement('option');
            noOptionsElement.value = '';
            noOptionsElement.textContent = 'No categories available';
            noOptionsElement.disabled = true;
            select.appendChild(noOptionsElement);
        }

        // Remove loading state
        this.elements.categorySelector.classList.remove('loading');

        // Add event listener
        select.addEventListener('change', () => {
            const selectedId = select.value;
            if (selectedId) {
                // Add visual feedback for selection
                this.elements.categorySelector.classList.add('has-selection');
                this.selectedRoot = this.categories.categories.find(cat => cat.id === selectedId);
                this.handleCategorySelect();
            } else {
                this.elements.categorySelector.classList.remove('has-selection');
                this.clearCategoryTree();
            }
        });
    }

    handleCategorySelect() {
        if (!this.selectedRoot) {
            this.clearCategoryTree();
            return;
        }

        this.buildCategoryTree();
        this.showSelectedCategoryLabel();
    }

    buildCategoryTree() {
        const createNode = (categoryData) => {
            const node = new CategoryNode(
                categoryData.id,
                categoryData.name?.en,
                categoryData.name?.bo,
                categoryData.name?.lzh,
                categoryData.description?.en,
                categoryData.description?.bo,
                categoryData.description?.lzh,
                categoryData.short_description?.en,
                categoryData.short_description?.bo,
                categoryData.short_description?.lzh
            );

            if (categoryData.subcategories) {
                Object.values(categoryData.subcategories).forEach(subcategory => {
                    const childNode = createNode(subcategory);
                    node.addChild(childNode);
                });
            }
            return node;
        };

        this.root = createNode(this.selectedRoot);
        this.renderTree();
    }

    showSelectedCategoryLabel() {
        if (this.elements.selectedCategoryLabel && this.selectedRoot) {
            this.elements.selectedCategoryLabel.textContent =
                `Selected: ${this.selectedRoot.name.en} (${this.selectedRoot.name.bo})`;
            this.elements.selectedCategoryLabel.style.display = 'block';
        }
    }

    clearCategoryTree() {
        if (this.elements.categoryTree) {
            this.elements.categoryTree.innerHTML = '';
        }
        if (this.elements.selectedCategoryLabel) {
            this.elements.selectedCategoryLabel.style.display = 'none';
        }
        if (this.elements.addCategoryBtn) {
            this.elements.addCategoryBtn.style.display = 'none';
        }
        this.selectedNode = null;
    }

    renderTree() {
        if (!this.elements.categoryTree || !this.root) return;

        this.elements.categoryTree.innerHTML = '';
        this.elements.categoryTree.appendChild(this.createNodeElement(this.root));
    }

    createNodeElement(node) {
        const nodeDiv = document.createElement('div');
        nodeDiv.className = 'tree-node';

        const contentDiv = document.createElement('div');
        contentDiv.className = 'node-content';

        const titleSpan = document.createElement('span');
        titleSpan.className = 'node-title';
        titleSpan.textContent = `${node.titleBo} (${node.titleEn}) ${node?.titleLzh ? `(${node?.titleLzh})` : ''}`;
        contentDiv.appendChild(titleSpan);

        // Create popup
        if (node.descBo || node.descEn || node.descLzh || node.shortdescBo || node.shortDescEn || node.shortDescLzh) {
            const popup = this.createNodePopup(node);
            contentDiv.appendChild(popup);
        }

        // Add click handler
        contentDiv.addEventListener('click', (e) => {
            e.stopPropagation();
            this.selectNode(node, contentDiv);
        });

        nodeDiv.appendChild(contentDiv);

        // Add children
        if (node.children.length > 0) {
            const childrenContainer = document.createElement('div');
            childrenContainer.className = 'children';
            node.children.forEach(child => {
                childrenContainer.appendChild(this.createNodeElement(child));
            });
            nodeDiv.appendChild(childrenContainer);
        }

        return nodeDiv;
    }

    createNodePopup(node) {
        const popup = document.createElement('div');
        popup.className = 'node-popup';

        if (node.descBo || node.descEn || node.descLzh) {
            const desc = document.createElement('div');
            desc.className = 'popup-section';
            desc.innerHTML = `
                <h5>Description</h5>
                ${node.descBo ? `<p><span class="label">BO</span> ${node.descBo}</p>` : ''}
                ${node.descEn ? `<p><span class="label">EN</span> ${node.descEn}</p>` : ''}
                ${node.descLzh ? `<p><span class="label">ZH</span> ${node.descLzh}</p>` : ''}
            `;
            popup.appendChild(desc);
        }

        if (node.shortdescBo || node.shortDescEn || node.shortDescLzh) {
            const shortDesc = document.createElement('div');
            shortDesc.className = 'popup-section';
            shortDesc.innerHTML = `
                <h5>Short Description</h5>
                ${node.shortdescBo ? `<p><span class="label">BO</span> ${node.shortdescBo}</p>` : ''}
                ${node.shortDescEn ? `<p><span class="label">EN</span> ${node.shortDescEn}</p>` : ''}
                ${node.shortDescLzh ? `<p><span class="label">ZH</span> ${node.shortDescLzh}</p>` : ''}
            `;
            popup.appendChild(shortDesc);
        }

        return popup;
    }

    selectNode(node, contentDiv) {
        this.selectedNode = node;

        // Remove selected class from all nodes
        document.querySelectorAll('.node-content').forEach(el => {
            el.classList.remove('selected');
        });

        // Add selected class
        contentDiv.classList.add('selected');

        // Show add button
        if (this.elements.addCategoryBtn) {
            this.elements.addCategoryBtn.style.display = 'inline-flex';
        }
    }

    async fetchPechaOptions() {
        try {
            console.log('Fetching pecha options...');
            this.toggleLoadingSpinner(true);

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
                    body: JSON.stringify({ page: currentPage, limit: limit })
                });

                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

                const pechas = await response.json();
                allPechas = allPechas.concat(pechas.metadata || []);
                hasMorePages = (pechas.metadata && pechas.metadata.length === limit);
                currentPage++;
            }

            this.populatePechaDropdown(allPechas);

        } catch (error) {
            console.error('Error fetching pecha options:', error);
            this.showToast('Failed to load pecha options. Please try again.', 'error');
        } finally {
            this.toggleLoadingSpinner(false);
        }
    }

    populatePechaDropdown(pechas) {
        if (!this.elements.pechaSelect) {
            console.warn('Pecha select element not found');
            return;
        }

        this.elements.pechaSelect.innerHTML = '<option value="">Select a pecha...</option>';

        if (!Array.isArray(pechas)) {
            console.warn('Invalid pechas data - not an array');
            return;
        }

        pechas.forEach(pecha => {
            try {
                const title = pecha.title?.[pecha.language] ?? pecha.title?.bo ?? 'Unknown Title';
                const option = new Option(`${pecha.id} - ${title}`, pecha.id);
                this.elements.pechaSelect.appendChild(option);
            } catch (error) {
                console.warn('Error processing pecha:', pecha, error);
            }
        });
    }

    toggleLoadingSpinner(isLoading) {
        // Try to get the element if it's not cached
        if (!this.elements.pechaLoadingSpinner) {
            this.elements.pechaLoadingSpinner = document.getElementById('pechaLoadingSpinner');
        }

        // If still not found, log warning and return
        if (!this.elements.pechaLoadingSpinner) {
            console.warn('Loading spinner element not found');
            return;
        }

        try {
            if (isLoading) {
                this.elements.pechaLoadingSpinner.classList.add('active');
            } else {
                this.elements.pechaLoadingSpinner.classList.remove('active');
            }
        } catch (error) {
            console.warn('Error toggling loading spinner:', error);
        }
    }

    async handlePechaSelect() {
        const pechaId = this.elements.pechaSelect?.value;

        if (!pechaId) {
            this.showInitialMetadataState();
            this.showInitialRelatedPechasState();
            return;
        }

        try {
            this.showLoadingState();
            this.showRelatedPechasLoadingState();

            // Fetch both metadata and related pechas in parallel
            const [metadata, relatedPechas] = await Promise.all([
                this.fetchMetadata(pechaId),
                this.fetchRelatedPechas(pechaId)
            ]);

            this.displayMetadata(metadata);
            this.displayRelatedPechas(relatedPechas, pechaId);
        } catch (error) {
            console.error('Error in handlePechaSelect:', error);
            this.showToast('Unable to fetch data. Please try again later.', 'error');
            this.showErrorState('Failed to load metadata. Please try again.');
            this.showRelatedPechasErrorState('Failed to load related pechas. Please try again.');
        }
    }

    async fetchMetadata(pechaId) {
        const response = await fetch(`${this.API_ENDPOINT}/metadata/${pechaId}`, {
            method: 'GET',
            headers: { 'accept': 'application/json' }
        });

        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        return await response.json();
    }

    showInitialMetadataState() {
        if (this.elements.metadataContent) {
            this.elements.metadataContent.innerHTML = `
                <div class="metadata-placeholder">
                    <i class="fas fa-info-circle"></i>
                    <p>Select a pecha to view its metadata</p>
                </div>
            `;
        }
    }

    showLoadingState() {
        if (this.elements.metadataContent) {
            this.elements.metadataContent.innerHTML = `
                <div class="metadata-placeholder">
                    <i class="fas fa-spinner fa-spin"></i>
                    <p>Loading metadata...</p>
                </div>
            `;
        }
    }

    showErrorState(message) {
        if (this.elements.metadataContent) {
            this.elements.metadataContent.innerHTML = `
                <div class="metadata-placeholder">
                    <i class="fas fa-exclamation-triangle" style="color: var(--error-color);"></i>
                    <p style="color: var(--error-color);">${message}</p>
                </div>
            `;
        }
    }

    displayMetadata(metadata) {
        if (!this.elements.metadataContent) return;

        const reorderedMetadata = this.reorderMetadata(metadata);
        let metadataHTML = '';

        for (const [key, value] of Object.entries(reorderedMetadata)) {
            if (!value || key === 'category') continue;

            const formattedKey = this.formatMetadataKey(key);
            const formattedValue = this.formatMetadataValue(value);

            metadataHTML += `
                <div class="metadata-item">
                    <div class="metadata-key">${formattedKey}</div>
                    <div class="metadata-value">${formattedValue}</div>
                </div>
            `;
        }

        // Handle category separately
        if (reorderedMetadata.category) {
            metadataHTML += `
                <div class="metadata-item" id="category-metadata-item">
                    <div class="metadata-key">CATEGORY</div>
                    <div class="metadata-value">
                        <div class="category-loading">
                            <div class="loading-spinner"></div>
                            <span>Loading category...</span>
                        </div>
                    </div>
                </div>
            `;
        }

        this.elements.metadataContent.innerHTML = metadataHTML;

        // Fetch category chain
        if (reorderedMetadata.category) {
            this.getCategoryChain(reorderedMetadata.category).then(categoryChain => {
                const categoryItem = document.getElementById('category-metadata-item');
                if (categoryItem) {
                    categoryItem.querySelector('.metadata-value').innerHTML = categoryChain;
                }
            }).catch(error => {
                console.error('Error updating category chain:', error);
                const categoryItem = document.getElementById('category-metadata-item');
                if (categoryItem) {
                    categoryItem.querySelector('.metadata-value').innerHTML =
                        `<span class="empty-value">${reorderedMetadata.category}</span>`;
                }
            });
        }
    }

    formatMetadataKey(key) {
        const keyMappings = {
            'title': 'Title',
            'version_of': 'Version Of',
            'commentary_of': 'Commentary Of',
            'translation_of': 'Translation Of',
            'language': 'Language',
            'author': 'Author',
            'source': 'Source',
            'long_title': 'Long Title',
            'document_id': 'Document ID',
            'usage_title': 'Usage Title',
            'alt_titles': 'Alternative Titles',
            'presentation': 'Presentation',
            'date': 'Date'
        };

        return keyMappings[key] || key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }

    formatMetadataValue(value) {
        if (value === null || value === undefined || value === '') {
            return '<span class="empty-value">Not specified</span>';
        }

        if (Array.isArray(value)) {
            if (value.length === 0) {
                return '<span class="empty-value">Not specified</span>';
            }

            // Handle array of objects with localized values
            if (typeof value[0] === 'object') {
                return `<div class="array-container">${value.map((item, index) => {
                    const entries = Object.entries(item);
                    if (entries.length === 0) return '<span class="empty-value">Not specified</span>';

                    const localizedValues = entries.map(([lang, text]) => `
                        <div class="localized-value">
                            <span class="language-tag">${lang.toUpperCase()}</span>
                            <span class="text">${this.escapeHtml(text)}</span>
                        </div>
                    `).join('');

                    return `<div class="array-item">${localizedValues}</div>`;
                }).join('')}</div>`;
            }

            // Handle array of strings
            return value.map(item =>
                `<span class="simple-value">${this.escapeHtml(item.toString())}</span>`
            ).join(' ');
        }

        if (typeof value === 'object') {
            const entries = Object.entries(value);
            if (entries.length === 0) {
                return '<span class="empty-value">Not specified</span>';
            }

            return entries.map(([lang, text]) => `
                <div class="localized-value">
                    <span class="language-tag">${lang.toUpperCase()}</span>
                    <span class="text">${this.escapeHtml(text)}</span>
                </div>
            `).join('');
        }

        return `<span class="simple-value">${this.escapeHtml(value.toString())}</span>`;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    reorderMetadata(metadata) {
        const order = [
            "title", "version_of", "commentary_of", "translation_of",
            "language", "author", "category", "source", "long_title",
            "document_id", "usage_title", "alt_titles", "presentation", "date"
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
                headers: { 'Content-Type': 'application/json' }
            });

            if (!response.ok) throw new Error('Network response was not ok');

            const data = await response.json();
            const chain = this.findCategoryChain(data.categories, categoryId);

            if (chain && chain.length > 0) {
                // Create a styled category chain with clear hierarchy
                const chainItems = chain.map(category => `<span class="category-item">${this.escapeHtml(category)}</span>`);
                return `<div class="category-chain">${chainItems.join('<span class="category-separator">›</span>')}</div>`;
            } else {
                return `<span class="empty-value">${categoryId}</span>`;
            }
        } catch (error) {
            console.error('Error fetching category chain:', error);
            return `<span class="empty-value">${categoryId}</span>`;
        }
    }

    findCategoryChain(data, targetId) {
        function searchInData(items, currentPath = []) {
            for (const item of items) {
                const newPath = [...currentPath, item.name['en']];

                if (item.id === targetId) {
                    return newPath;
                }

                if (item.subcategories && item.subcategories.length > 0) {
                    const result = searchInData(item.subcategories, newPath);
                    if (result) return result;
                }
            }
            return null;
        }
        return searchInData(data);
    }

    toggleMetadata() {
        if (!this.elements.metadataContent || !this.elements.toggleBtn) return;

        this.isMetadataCollapsed = !this.isMetadataCollapsed;
        this.elements.metadataContent.classList.toggle('collapsed');
        this.elements.toggleBtn.classList.toggle('collapsed');
    }

    toggleRelatedPechas() {
        if (!this.elements.relatedPechasContent || !this.elements.relatedPechasToggleBtn) return;

        this.isRelatedPechasCollapsed = !this.isRelatedPechasCollapsed;
        this.elements.relatedPechasContent.classList.toggle('collapsed');
        this.elements.relatedPechasToggleBtn.classList.toggle('collapsed');
    }

    async fetchRelatedPechas(pechaId) {
        try {
            console.log(`Fetching related pechas for: ${pechaId}`);
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${pechaId}/related?traversal=full_tree`, {
                method: 'GET',
                headers: { 'accept': 'application/json' }
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            console.log('Related pechas data:', data);
            return data;
        } catch (error) {
            console.error('Error fetching related pechas:', error);
            throw error;
        }
    }

    showInitialRelatedPechasState() {
        if (this.elements.relatedPechasContent) {
            this.elements.relatedPechasContent.innerHTML = `
                <div class="related-pechas-placeholder">
                    <i class="fas fa-link"></i>
                    <p>Select a pecha to view related texts</p>
                </div>
            `;
        }
    }

    showRelatedPechasLoadingState() {
        if (this.elements.relatedPechasContent) {
            this.elements.relatedPechasContent.innerHTML = `
                <div class="related-pechas-loading">
                    <i class="fas fa-spinner fa-spin"></i>
                    <p>Loading related pechas...</p>
                </div>
            `;
        }
    }

    showRelatedPechasErrorState(message) {
        if (this.elements.relatedPechasContent) {
            this.elements.relatedPechasContent.innerHTML = `
                <div class="related-pechas-error">
                    <i class="fas fa-exclamation-triangle"></i>
                    <p>${message}</p>
                </div>
            `;
        }
    }

    displayRelatedPechas(relatedData, currentPechaId) {
        if (!this.elements.relatedPechasContent) return;

        // Check if we have related pechas data
        if (!relatedData || !Array.isArray(relatedData) || relatedData.length === 0) {
            this.elements.relatedPechasContent.innerHTML = `
                <div class="related-pechas-empty">
                    <i class="fas fa-unlink"></i>
                    <p>No related pechas found</p>
                </div>
            `;
            this.selectedRelatedPechas = [];
            return;
        }

        // Filter out the current pecha from the related list
        const filteredRelated = relatedData.filter(item => item.id !== currentPechaId);

        if (filteredRelated.length === 0) {
            this.elements.relatedPechasContent.innerHTML = `
                <div class="related-pechas-empty">
                    <i class="fas fa-unlink"></i>
                    <p>No related pechas found</p>
                </div>
            `;
            this.selectedRelatedPechas = [];
            return;
        }

        // Initialize selected related pechas - select all by default
        this.selectedRelatedPechas = filteredRelated.map(pecha => pecha.id);

        // Create the related pechas list
        let relatedPechasHTML = '<div class="related-pechas-list">';

        // Add select all/none controls
        relatedPechasHTML += `
            <div class="related-pechas-controls">
                <button class="btn-select-all" type="button">
                    <i class="fas fa-check-square"></i> Select All
                </button>
                <button class="btn-select-none" type="button">
                    <i class="fas fa-square"></i> Select None
                </button>
                <span class="selection-count">${filteredRelated.length} of ${filteredRelated.length} selected</span>
            </div>
        `;

        filteredRelated.forEach(pecha => {
            const title = pecha.title;
            const relationship = this.determineRelationship(pecha);
            const isSelected = this.selectedRelatedPechas.includes(pecha.id);

            relatedPechasHTML += `
                <div class="related-pecha-item ${isSelected ? 'selected' : ''}" data-pecha-id="${pecha.id}">
                    <div class="related-pecha-checkbox">
                        <i class="fas ${isSelected ? 'fa-check-square' : 'fa-square'}"></i>
                    </div>
                    <div class="related-pecha-content">
                        <div class="related-pecha-header">
                            <span class="related-pecha-id">${pecha.id}</span>
                        </div>
                        <div class="related-pecha-title">${this.escapeHtml(title)}</div>
                        <div class="related-pecha-meta">
                            ${relationship ? `<span class="related-pecha-relationship">${relationship}</span>` : ''}
                        </div>
                    </div>
                </div>
            `;
        });

        relatedPechasHTML += '</div>';
        this.elements.relatedPechasContent.innerHTML = relatedPechasHTML;

        // Add click event listeners to related pecha items and controls
        this.setupRelatedPechaClickHandlers();
    }

    setupRelatedPechaClickHandlers() {
        const relatedPechaItems = this.elements.relatedPechasContent.querySelectorAll('.related-pecha-item');
        const selectAllBtn = this.elements.relatedPechasContent.querySelector('.btn-select-all');
        const selectNoneBtn = this.elements.relatedPechasContent.querySelector('.btn-select-none');

        relatedPechaItems.forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();
                const pechaId = item.dataset.pechaId;
                if (pechaId) {
                    this.toggleRelatedPechaSelection(pechaId, item);
                }
            });
        });

        // Select All button
        if (selectAllBtn) {
            selectAllBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.selectAllRelatedPechas();
            });
        }

        // Select None button
        if (selectNoneBtn) {
            selectNoneBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.selectNoneRelatedPechas();
            });
        }
    }

    toggleRelatedPechaSelection(pechaId, itemElement) {
        const isCurrentlySelected = this.selectedRelatedPechas.includes(pechaId);

        if (isCurrentlySelected) {
            // Remove from selection
            this.selectedRelatedPechas = this.selectedRelatedPechas.filter(id => id !== pechaId);
            itemElement.classList.remove('selected');
            const checkbox = itemElement.querySelector('.related-pecha-checkbox i');
            if (checkbox) {
                checkbox.className = 'fas fa-square';
            }
        } else {
            // Add to selection
            this.selectedRelatedPechas.push(pechaId);
            itemElement.classList.add('selected');
            const checkbox = itemElement.querySelector('.related-pecha-checkbox i');
            if (checkbox) {
                checkbox.className = 'fas fa-check-square';
            }
        }

        this.updateSelectionCount();
    }

    selectAllRelatedPechas() {
        const allItems = this.elements.relatedPechasContent.querySelectorAll('.related-pecha-item');

        allItems.forEach(item => {
            const pechaId = item.dataset.pechaId;
            if (pechaId && !this.selectedRelatedPechas.includes(pechaId)) {
                this.selectedRelatedPechas.push(pechaId);
            }
            item.classList.add('selected');
            const checkbox = item.querySelector('.related-pecha-checkbox i');
            if (checkbox) {
                checkbox.className = 'fas fa-check-square';
            }
        });

        this.updateSelectionCount();
    }

    selectNoneRelatedPechas() {
        const allItems = this.elements.relatedPechasContent.querySelectorAll('.related-pecha-item');

        this.selectedRelatedPechas = [];

        allItems.forEach(item => {
            item.classList.remove('selected');
            const checkbox = item.querySelector('.related-pecha-checkbox i');
            if (checkbox) {
                checkbox.className = 'fas fa-square';
            }
        });

        this.updateSelectionCount();
    }

    updateSelectionCount() {
        const selectionCountElement = this.elements.relatedPechasContent.querySelector('.selection-count');
        const totalItems = this.elements.relatedPechasContent.querySelectorAll('.related-pecha-item').length;

        if (selectionCountElement) {
            selectionCountElement.textContent = `${this.selectedRelatedPechas.length} of ${totalItems} selected`;
        }
    }

    determineRelationship(pecha) {
        // Determine relationship type based on pecha metadata
        // This would depend on the actual API response structure
        if (pecha.relationship_type) {
            return pecha.relationship_type;
        }

        if (pecha.version_of) {
            return 'Version of ' + pecha.version_of;
        }

        if (pecha.commentary_of) {
            return 'Commentary of ' + pecha.commentary_of;
        }

        if (pecha.translation_of) {
            return 'Translation of ' + pecha.translation_of;
        }

        return 'Root';
    }

    async assignCategory() {
        try {
            // Check if a specific category node is selected from the tree
            if (!this.selectedNode) {
                this.showToast('Please select a specific category from the category tree first', 'error');
                return;
            }

            const selectedPechaId = this.elements.pechaSelect?.value;
            if (!selectedPechaId) {
                this.showToast('Please select a pecha first', 'error');
                return;
            }

            const selectedDestination = document.querySelector('input[name="destination"]:checked');
            if (!selectedDestination) {
                this.showToast('Please select a destination', 'error');
                return;
            }

            const requestBody = {
                category_id: this.selectedNode.id,
                site: selectedDestination.value,
                relate_pecha: this.selectedRelatedPechas
            };

            console.log('Assigning category:', requestBody);
            console.log('Selected node:', this.selectedNode);
            console.log('Selected related pechas:', this.selectedRelatedPechas);
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${selectedPechaId}/category`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) throw new Error('Network response was not ok');

            const data = await response.json();
            console.log('Category assigned successfully:', data);

            const relatedPechasText = this.selectedRelatedPechas.length > 0
                ? ` with ${this.selectedRelatedPechas.length} related pecha(s)`
                : '';
            this.showToast(`Category "${this.selectedNode.titleEn}" assigned successfully${relatedPechasText}`, 'success');

            // Refresh metadata to show the updated category
            this.handlePechaSelect();

        } catch (error) {
            console.error('Error assigning category:', error);
            this.showToast('Failed to assign category. Please try again.', 'error');
        }
    }

    showToast(message, type = 'info') {
        // Create toast container if it doesn't exist
        let toastContainer = this.elements.toastContainer;
        if (!toastContainer) {
            toastContainer = document.createElement('div');
            toastContainer.id = 'toastContainer';
            toastContainer.setAttribute('role', 'alert');
            toastContainer.setAttribute('aria-live', 'polite');
            document.body.appendChild(toastContainer);
            this.elements.toastContainer = toastContainer;
        }

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;

        const icon = this.getToastIcon(type);
        const content = document.createElement('div');
        content.className = 'toast-content';
        content.textContent = message;

        toast.appendChild(icon);
        toast.appendChild(content);

        toastContainer.appendChild(toast);

        // Auto dismiss after 5 seconds with animation
        setTimeout(() => {
            if (toast.parentNode) {
                toast.classList.add('dismissing');
                setTimeout(() => {
                    if (toast.parentNode) {
                        toast.remove();
                    }
                }, 300);
            }
        }, 5000);
    }

    getToastIcon(type) {
        const iconElement = document.createElement('i');
        iconElement.className = 'fas';

        switch (type) {
            case 'success':
                iconElement.classList.add('fa-check-circle');
                break;
            case 'error':
                iconElement.classList.add('fa-exclamation-circle');
                break;
            case 'warning':
                iconElement.classList.add('fa-exclamation-triangle');
                break;
            default:
                iconElement.classList.add('fa-info-circle');
        }

        return iconElement;
    }
}

// Initialize the application
document.addEventListener('DOMContentLoaded', () => {
    console.log('Initializing Category Manager...');
    window.categoryManager = new CategoryManager();
}); 