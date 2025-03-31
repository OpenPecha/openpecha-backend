// Category Node Data Structure
class CategoryNode {
    constructor(id, titleEn, titleBo, descEn = '', descBo = '', shortDescEn = '', shortdescBo = '') {
        this.id = id;
        this.titleEn = titleEn;
        this.titleBo = titleBo;
        this.descEn = descEn;
        this.descBo = descBo;
        this.shortDescEn = shortDescEn;
        this.shortdescBo = shortdescBo;
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
            toastContainer: document.getElementById('toastContainer')
        };
        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app/';
        this.root = null;
        this.currentLanguage = 'en';
        this.selectedRoot = null;
        this.selectedNode = null;
        this.categories = [];
        this.options = [];
        this.bindEventListeners();
        this.fetchCategories();
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
        const addBtn = document.getElementById('addCategory');
        const cancelBtn = document.getElementById('cancelCategory');
        const categoryForm = document.getElementById('categoryForm');
        const langToggle = document.getElementById('languageToggle');

        // Add event listeners with error handling
        if (addBtn) {
            addBtn.addEventListener('click', () => {
                this.assignCategory();
            });
        }

        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => {
                this.selectedNode = null;
                const container = document.getElementById('categoryTree');
                container.innerHTML = '';   
                categoryForm.reset();
            });
        }

        if (categoryForm) {
            categoryForm.addEventListener('submit', (e) => this.handleFormSubmit(e));
        }

        if (langToggle) {
            langToggle.addEventListener('click', () => this.toggleLanguage());
        }

        if (this.elements.categorySelector) {
            this.elements.categorySelector.addEventListener('customDropdownChange', () => {
                this.handleCategorySelect();
            });
        }
    }

    fetchCategories() {
        console.log('CategoryTreeUI: Fetching categories...');
        fetch(this.API_ENDPOINT + 'categories', {
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
            console.log("categories :: ",data)
            this.categories = data;
            this.options = this.extractCategoryNames(data, this.currentLanguage);
            this.elements.categorySelector.innerHTML = '';
            new CustomSearchableDropdown(this.elements.categorySelector, this.options, 'selectedCategory');
        })
        .catch(error => {
            console.error('CategoryTreeUI: Error fetching categories', error);
        });
    }

    extractCategoryNames(data, lang) {
        return data.categories.map(category => ({
            id: category.id,
            name: category.name[lang] || category.id // Fallback to id if name is missing
        }));
    }

    handleCategorySelect() {
        const selectedRoot = document.getElementById('selectedCategory').dataset.value;
        console.log(":::",selectedRoot)
        this.selectedRoot = this.categories.categories.find(category => category.id === selectedRoot);
        // console.log(":::",this.selectedRoot)
        // this.displayCategory(this.selectedRoot);
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
                categoryData.description?.en,
                categoryData.description?.bo,
                categoryData.short_description?.en,
                categoryData.short_description?.bo
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
        if(!this.selectedNode) {
            this.showToast('Please select the destination category first', 'error');
            return;
        }
        console.log("selected node ",this.selectedNode.id)
        const selectedPecha = document.getElementById('selectedPecha');
        if (!selectedPecha) {
            alert("Please select a pecha first");
            return;
        }   
        fetch(`${this.API_ENDPOINT}/metadata/${selectedPecha.dataset.value}/category`, {
            method: 'PUT',
            headers: {
                'accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ category_id: this.selectedNode.id })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            console.log('Category assigned successfully:', data);
            document.querySelectorAll('.node-content').forEach(el => {
                el.classList.remove('selected');
                el.style.transform = '';
            });
            this.selectedNode = null;
            const addBtn = document.getElementById('addCategory');
            addBtn.style.display = 'none';
            this.renderTree();
            this.showToast('Category assigned successfully', 'success');
        })
        .catch(error => {
            console.error('Error assigning category:', error);
        });
    }

    toggleLanguage() {
        this.currentLanguage = this.currentLanguage === 'en' ? 'bo' : 'en';
        const langText = document.querySelector('.lang-text');
        if (langText) {
            langText.textContent = this.currentLanguage;
        }
        this.options = this.extractCategoryNames(this.categories, this.currentLanguage);
        this.elements.categorySelector.innerHTML = '';
        const addBtn = document.getElementById('addCategory');
        addBtn.style.display = 'none';
        new CustomSearchableDropdown(this.elements.categorySelector, this.options, 'selectedCategory');
        this.renderTree();
    }

    createNodeElement(node) {
        const nodeDiv = document.createElement('div');
        nodeDiv.className = 'tree-node';

        const contentDiv = document.createElement('div');
        contentDiv.className = 'node-content';
        
        // Add node title with language-specific class
        const titleSpan = document.createElement('span');
        titleSpan.className = `node-title ${this.currentLanguage}`;
        titleSpan.textContent = this.currentLanguage === 'en' ? node.titleEn : node.titleBo;
        contentDiv.appendChild(titleSpan);

        // Create popup with improved structure
        const popup = document.createElement('div');
        popup.className = 'node-popup';
        
        const title = document.createElement('h4');
        title.innerHTML = `
            <div class="popup-title">
                <span class="label">Tibetan:</span> ${node.titleBo}
            </div>
            <div class="popup-title">
                <span class="label">English:</span> ${node.titleEn}
            </div>
        `;
        popup.appendChild(title);

        if (node.descBo || node.descEn) {
            const desc = document.createElement('div');
            desc.className = 'popup-section';
            desc.innerHTML = `
                <h5>Description</h5>
                ${node.descBo ? `<p><span class="label">Tibetan:</span> ${node.descBo}</p>` : ''}
                ${node.descEn ? `<p><span class="label">English:</span> ${node.descEn}</p>` : ''}
            `;
            popup.appendChild(desc);
        }

        if (node.shortdescBo || node.shortDescEn) {
            const shortDesc = document.createElement('div');
            shortDesc.className = 'popup-section';
            shortDesc.innerHTML = `
                <h5>Short Description</h5>
                ${node.shortdescBo ? `<p><span class="label">Tibetan:</span> ${node.shortdescBo}</p>` : ''}
                ${node.shortDescEn ? `<p><span class="label">English:</span> ${node.shortDescEn}</p>` : ''}
            `;
            popup.appendChild(shortDesc);
        }

        contentDiv.appendChild(popup);

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
        this.elements = {
            form: document.getElementById('publishForm'),
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            pechaSelect: document.getElementById('pechaOptions'),
            toastContainer: document.getElementById('toastContainer'),
            metadataContainer: document.querySelector('.metadata-container'),
            metadataContent: document.querySelector('.metadata-content'),
            metadataHeader: document.querySelector('.metadata-header'),
            toggleIcon: document.querySelector('.toggle-icon'),
            treeContainer: document.querySelector('.tree-container')
        };

        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.isLoading = false;
        this.isCollapsed = false;
        
        // Initialize components
        this.setupEventListeners();
        this.showInitialMetadataState();
        this.fetchPechaOptions();
    }

    setupEventListeners() {
        // Listen for changes on the custom dropdown
        if (this.elements.pechaOptionsContainer) {
            this.elements.pechaOptionsContainer.addEventListener('customDropdownChange', () => {
                this.handlePechaSelect();
            });
        }

        if (this.elements.form) {
            this.elements.form.addEventListener('submit', (e) => {
                e.preventDefault();
                this.handlePublish();
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
        this.showSelectLoading(true);

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
        } finally {
            this.showSelectLoading(false);
        }
    }

    updatePechaOptions(pechas) {
        if (this.elements.pechaSelect) {
            this.elements.pechaSelect.style.display = 'none';
            new CustomSearchableDropdown(this.elements.pechaOptionsContainer, pechas, "selectedPecha");
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
        const selectedPecha = document.getElementById("selectedPecha");
        if (!selectedPecha) return

        const pechaId = selectedPecha.dataset.value;
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

        this.elements.metadataContent.innerHTML = metadataHTML;
    }

    showTreeState(show = true) {
        if (!this.elements.treeContainer) return;
        this.elements.treeContainer.style.display = show ? 'block' : 'none';
    }

    toggleMetadata() {
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
