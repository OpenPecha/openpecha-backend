// Category Node Data Structure
class CategoryNode {
    constructor(id, titleEn, titleBo, titleLzh, descEn = '', descBo = '', descLzh='', shortDescEn = '', shortdescBo = '',shortDescLzh='') {
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

// Category Tree UI Management
class CategoryTreeUI {
    constructor() {
        console.log('CategoryTreeUI: Initializing...');
        this.elements = {
            categorySelector: document.getElementById('categorySelector'),
            langToggle: document.getElementById('languageToggle'),
            toastContainer: document.getElementById('toastContainer'),
            addCategory: document.getElementById('addCategory')
        };
        this.API_ENDPOINT = '';
        this.root = null;
        this.currentLanguage = 'en';
        this.selectedRoot = null;
        this.selectedNode = null;
        this.categories = [];
        this.options = [];
        getApiEndpoint().then((apiEndpoint) => {
            this.API_ENDPOINT = apiEndpoint;
            this.bindEventListeners();
            this.fetchCategories();
        });
    }

    bindEventListeners() {
        console.log('CategoryTreeUI: Binding event listeners...');
        // Wait for DOM to be ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.init());
        } else {
            this.init();
        }
    }

    init() {
        console.log('CategoryTreeUI: DOM ready, initializing components...');
        // Get UI elements
        const categoryForm = document.getElementById('categoryForm');
        
        if (categoryForm) {
            categoryForm.addEventListener('submit', (e) => this.handleFormSubmit(e));
        }

        if (this.elements.categorySelector) {
            this.elements.categorySelector.addEventListener('customDropdownChange', () => {
                this.handleCategorySelect();
            });
        }

        if (this.elements.addCategory) {
            this.elements.addCategory.addEventListener('click', () => {
                this.assignCategory();
            });
        }
    }

    fetchCategories() {
        console.log('CategoryTreeUI: Fetching categories...');
        fetch(`${this.API_ENDPOINT}/categories`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            console.log('CategoryTreeUI: Categories fetched successfully');
            this.categories = data;
            console.log("data",data)
            this.options = this.extractCategoryNames(data);
            this.elements.categorySelector.innerHTML = '';
            new CustomSearchableDropdown(this.elements.categorySelector, this.options, 'selectedCategory');
        })
        .catch(error => {
            console.error('CategoryTreeUI: Error fetching categories', error);
        });
    }

    extractCategoryNames(data) {
        return data.categories.map(category => ({
            id: category.id,
            name: `${category.name["bo"]} (${category.name["en"]}) ${category?.name["lzh"]?"("+category?.name["lzh"]+")":""}` 
        }));
    }
    
    handleCategorySelect() {
        const selectedRoot = document.getElementById('selectedCategory').dataset.value;
        this.selectedRoot = this.categories.categories.find(category => category.id === selectedRoot);
        this.handleFormSubmit();
    }

    displayCategory(category) {
        const form = document.getElementById('categoryForm');
        if (!form) {
            console.error('CategoryTreeUI: Form not found');
            return;
        }
        
        // Reset form
        form.reset();
        
        // Fill form fields
        const titleEn = document.getElementById('titleEn');
        const titleBo = document.getElementById('titleBo');
        const descEn = document.getElementById('descEn');
        const shortDescEn = document.getElementById('shortDescEn');
        const descBo = document.getElementById('descBo');
        const shortDescBo = document.getElementById('shortDescBo');
        
        if (titleEn) titleEn.value = category.name?.en || '';
        if (titleBo) titleBo.value = category.name?.bo || '';
        if (descEn) descEn.value = category.description?.en || '';
        if (shortDescEn) shortDescEn.value = category.short_description?.en || '';
        if (descBo) descBo.value = category.description?.bo || '';
        if (shortDescBo) shortDescBo.value = category.short_description?.bo || '';
    }

    handleFormSubmit(e) {
        // e.preventDefault();
        if(!this.selectedRoot) {
            const container = document.getElementById('categoryTree');
            container.innerHTML = '';
            const addBtn = document.getElementById('addCategory');
            addBtn.style.display = 'none';
            return;
        }
        this.root = null;
        const createNode = (categoryData) => {
            console.log("categoryData",categoryData)
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
    
            // Recursively process subcategories
            if (categoryData.subcategories) {
                Object.values(categoryData.subcategories).forEach(subcategory => {
                    const childNode = createNode(subcategory);
                    node.addChild(childNode);
                });
            }
            return node;
        };
    
        const newNode = createNode(this.selectedRoot);
    
        if (!this.root) {
            this.root = newNode;
        } else if (this.selectedNode) {
            this.selectedNode.addChild(newNode);
        } else {
            alert('Please select a parent category first');
            return;
        }
        const label = document.querySelector('.selected-category');
        label.style.display = 'block';
    
        this.renderTree();
    }
    
    assignCategory() {
        if (!this.selectedRoot) {
            this.showToast('Please select a parent category first', 'error');
            return;
        }

        const selectedPechaId = document.getElementById('pechaOptions').value;
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
            category_id: this.selectedRoot.id,
            site: selectedDestination.value
        };

        console.log("assigning category", requestBody);
        fetch(`${this.API_ENDPOINT}/pechas/${selectedPechaId}/category`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            console.log('Category assigned successfully:', data);
            this.showToast('Category assigned successfully', 'success');
            // Refresh metadata display
            window.updateMetadata.handlePechaSelect();
        })
        .catch(error => {
            console.error('Error assigning category:', error);
        });
    }

    createNodeElement(node) {
        console.log("node",node)
        const nodeDiv = document.createElement('div');
        nodeDiv.className = 'tree-node';

        const contentDiv = document.createElement('div');
        contentDiv.className = 'node-content';
        
        // Add node title with language-specific class
        const titleSpan = document.createElement('span');
        titleSpan.className = `node-title`;
        titleSpan.textContent = `${node.titleBo} (${node.titleEn}) ${node?.titleLzh ? `(${node?.titleLzh})` : ''}`;
        contentDiv.appendChild(titleSpan);
        
        // Create popup with improved structure
        const popup = document.createElement('div');
        popup.className = 'node-popup';

        if (node.descBo || node.descEn || node.descLzh) {
            const desc = document.createElement('div');
            desc.className = 'popup-section';
            desc.innerHTML = `
                <h5>Description</h5>
                ${node.descBo ? `<p><span class="label">Tibetan:</span> ${node.descBo}</p>` : ''}
                ${node.descEn ? `<p><span class="label">English:</span> ${node.descEn}</p>` : ''}
                ${node.descLzh ? `<p><span class="label">Literal Chinese:</span> ${node.descLzh}</p>` : ''}
            `;
            popup.appendChild(desc);
        }

        if (node.shortdescBo || node.shortDescEn || node.shortDescLzh) {
            const shortDesc = document.createElement('div');
            shortDesc.className = 'popup-section';
            shortDesc.innerHTML = `
                <h5>Short Description</h5>
                ${node.shortdescBo ? `<p><span class="label">Tibetan:</span> ${node.shortdescBo}</p>` : ''}
                ${node.shortDescEn ? `<p><span class="label">English:</span> ${node.shortDescEn}</p>` : ''}
                ${node.shortDescLzh ? `<p><span class="label">Literal Chinese:</span> ${node.shortDescLzh}</p>` : ''}
            `;
            popup.appendChild(shortDesc);
        }

        if (node.descBo || node.descEn || node.descLzh || node.shortdescBo || node.shortDescEn || node.shortDescLzh) {
            contentDiv.appendChild(popup);
        }

        // Add click handler with visual feedback
        contentDiv.addEventListener('click', (e) => {
            e.stopPropagation();
            this.selectedNode = node;
            
            // Remove selected class from all nodes
            document.querySelectorAll('.node-content').forEach(el => {
                el.classList.remove('selected');
                el.style.transform = '';
            });
            
            // Add selected class and transform
            contentDiv.classList.add('selected');
            contentDiv.style.transform = 'translateX(5px)';
            
            // show add button
            const addBtn = document.getElementById('addCategory');
            addBtn.style.display = 'block';
            // Add ripple effect
            const ripple = document.createElement('div');
            ripple.className = 'ripple';
            contentDiv.appendChild(ripple);
            
            // Remove ripple after animation
            setTimeout(() => ripple.remove(), 1000);
        });

        nodeDiv.appendChild(contentDiv);

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

    renderTree() {
        const container = document.getElementById('categoryTree');
        if (!container) {
            console.error('CategoryTreeUI: Tree container not found');
            return;
        }

        container.innerHTML = '';
        if (this.root) {
            container.appendChild(this.createNodeElement(this.root));
        }
    }

    showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `${this.getToastIcon(type)} ${message}`;
        this.elements.toastContainer.appendChild(toast);

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
}

// Metadata Management
class UpdateMetaData {
    constructor() {
        console.log('UpdateMetaData: Initializing...');
        this.initialize();
    }

    async initialize() {
        try {
            this.setupElements();
            this.API_ENDPOINT = await getApiEndpoint();
            this.setupEventListeners();
            this.initializeSearchUI();
            this.fetchPechaOptions();
            this.showInitialMetadataState();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }

    setupElements() {
        console.log('UpdateMetaData: Setting up elements...');
        this.elements = {
            form: document.getElementById('publishForm'),
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            searchContainers: document.querySelectorAll('.select-search-container'),
            pechaLoadingSpinner: document.getElementById('pechaLoadingSpinner'),
            pechaSelect: document.getElementById('pechaOptions'),
            metadataContainer: document.querySelector('.metadata-container'),
            metadataContent: document.querySelector('.metadata-content'),
            metadataHeader: document.querySelector('.metadata-header'),
            toggleIcon: document.querySelector('.toggle-icon'),
            toastContainer: document.getElementById('toastContainer'),
            treeContainer: document.querySelector('.tree-container')
        };
        this.isLoading = false;
        this.isCollapsed = false;
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
                    this.navigateSearchResults(searchResults, 'next');
                } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    this.navigateSearchResults(searchResults, 'prev');
                } else if (e.key === 'Enter') {
                    e.preventDefault();
                    const activeItem = searchResults.querySelector('.search-item.active');
                    if (activeItem) {
                        activeItem.click();
                    }
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
            throw error;
        }
    }

    setupEventListeners() {
        
        if (this.elements.pechaOptionsContainer) {
            this.elements.pechaSelect.addEventListener('change', () => {
                this.handlePechaSelect();
            });
        }

        // Add toggle functionality
        if (this.elements.metadataHeader) {
            this.elements.metadataHeader.addEventListener('click', () => {
                this.toggleMetadata();
            });
        }
    }

    async fetchPechaOptions() {
        console.log('UpdateMetaData: Fetching pecha options...');
        try {
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
                    body: JSON.stringify({
                        page: currentPage,
                        limit: limit
                    })
                });

                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }

                const pechas = await response.json();
                allPechas = allPechas.concat(pechas.metadata);
                hasMorePages = pechas.metadata.length === limit;
                currentPage++;
            }

            this.populatePechaDropdown(allPechas);
        } catch (error) {
            console.error('UpdateMetaData: Error fetching pecha options:', error);
            this.showToast('Failed to load pecha options. Please try again.', 'error');
        } finally {
            this.toggleLoadingSpinner(false);
        }
    }

    populatePechaDropdown(pechas) {
        if (!this.elements.pechaSelect) return;

        this.elements.pechaSelect.innerHTML = '<option value="">Select pecha</option>';
        pechas.forEach(pecha => {
            const title = pecha.title[pecha.language] ?? pecha.title.bo;
            const option = new Option(`${pecha.id} - ${title}`, pecha.id);
            this.elements.pechaSelect.add(option.cloneNode(true));
        });
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

    showSelectLoading(isLoading) {
        if (!this.elements.pechaSelect) return;

        this.elements.pechaSelect.disabled = isLoading;
        if (isLoading) {
            const loadingOption = document.createElement('option');
            loadingOption.value = '';
            loadingOption.textContent = 'Loading pechas...';
            this.elements.pechaSelect.innerHTML = '';
            this.elements.pechaSelect.appendChild(loadingOption);
        }
    }

    async handlePechaSelect() {
        const pechaId = this.elements.pechaSelect.value;
        if (!pechaId) {
            this.showInitialMetadataState();
            this.showTreeState(false);  
            return;
        }

        try {
            this.showLoadingState();
            const metadata = await this.fetchMetadata(pechaId);
            this.displayMetadata(metadata);
            this.showTreeState(true);
        } catch (error) {
            console.error('Error in handlePechaSelect:', error);
            this.showToast('Unable to fetch metadata. Please try again later.', 'error');
            this.showErrorState('Failed to load metadata. Please try again.');
        }
    }

    async fetchMetadata(pechaId) {
        const response = await fetch(`${this.API_ENDPOINT}/metadata/${pechaId}`, {
            method: 'GET',
            headers: {
                'accept': 'application/json'
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        return await response.json();
    }

    showInitialMetadataState() {
        if (this.elements.metadataContent) {
            this.elements.metadataContent.innerHTML = `
                <div class="metadata-placeholder">
                    <p>Select a pecha to view metadata</p>
                </div>
            `;
        }
    }

    showLoadingState() {
        if (this.elements.metadataContent) {
            this.elements.metadataContent.innerHTML = `
                <div class="metadata-loading">
                    <div class="loading-spinner"></div>
                    <p>Loading metadata...</p>
                </div>
            `;
        }
    }

    showErrorState(message) {
        if (this.elements.metadataContent) {
            this.elements.metadataContent.innerHTML = `
                <div class="metadata-error">
                    <p>${message}</p>
                </div>
            `;
        }
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
        if (!this.elements.metadataContent) return;

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
        
        this.elements.metadataContent.innerHTML = metadataHTML;
        
        // Now fetch and update the category chain if needed
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
                    categoryItem.querySelector('.metadata-value').innerHTML = reorderedMetadata.category;
                }
            });
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
    
    showTreeState(show = true) {
        if (!this.elements.treeContainer) return;
        this.elements.treeContainer.style.display = show ? 'block' : 'none';
    }

    toggleMetadata() {
        console.log("toggle metadata")

        if (!this.elements.metadataContent || !this.elements.toggleIcon) return;

        this.isCollapsed = !this.isCollapsed;
        this.elements.metadataContent.classList.toggle('collapsed');
        this.elements.toggleIcon.classList.toggle('collapsed');
    }

    showToast(message, type = 'info') {
        if (!this.elements.toastContainer) return;

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        this.elements.toastContainer.appendChild(toast);

        setTimeout(() => {
            toast.remove();
        }, 3000);
    }
}

// Initialize both classes when the DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    console.log('Initializing application...');
    window.updateMetadata = new UpdateMetaData();
    window.categoryTree = new CategoryTreeUI();
});
