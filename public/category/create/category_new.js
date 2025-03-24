// Category Node Data Structure
class CategoryNode {
    constructor(titleEn, titleBo, descEn = '', descBo = '', shortDescEn = '', shortdescBo = '') {
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
        this.root = null;
        this.currentLanguage = 'en';
        this.selectedNode = null;
        this.bindEventListeners();
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
            console.log('CategoryTreeUI: Found Add Category button');
            addBtn.addEventListener('click', () => {
                console.log('CategoryTreeUI: Add button clicked');
                this.showModal();
            });
        } else {
            console.error('CategoryTreeUI: Add Category button not found');
        }

        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => {
                console.log('CategoryTreeUI: Cancel button clicked');
                this.hideModal();
            });
        }

        if (categoryForm) {
            categoryForm.addEventListener('submit', (e) => this.handleFormSubmit(e));
        }

        if (langToggle) {
            langToggle.addEventListener('click', () => this.toggleLanguage());
        }

        this.initFormValidation();
    }

    initFormValidation() {
        const descBo = document.getElementById('descBo');
        const descEn = document.getElementById('descEn');
        const shortdescBo = document.getElementById('shortdescBo');
        const shortDescEn = document.getElementById('shortDescEn');

        if (!descBo || !descEn || !shortdescBo || !shortDescEn) {
            console.error('CategoryTreeUI: Form elements not found');
            return;
        }

        // Validate Tibetan description requirements
        descBo.addEventListener('input', () => {
            if (descBo.value.trim() === '') {
                descEn.value = '';
                descEn.disabled = true;
            } else {
                descEn.disabled = false;
            }
        });

        shortdescBo.addEventListener('input', () => {
            if (shortdescBo.value.trim() === '') {
                shortDescEn.value = '';
                shortDescEn.disabled = true;
            } else {
                shortDescEn.disabled = false;
            }
        });
    }

    showModal() {
        console.log('CategoryTreeUI: Showing modal');
        const modal = document.getElementById('categoryModal');
        if (!modal) {
            console.error('CategoryTreeUI: Modal not found');
            return;
        }

        modal.style.display = 'block';
        // Add show class after a brief delay to trigger animation
        setTimeout(() => modal.classList.add('show'), 10);
        
        const form = document.getElementById('categoryForm');
        if (form) form.reset();

        const descEn = document.getElementById('descEn');
        const shortDescEn = document.getElementById('shortDescEn');
        
        if (descEn) descEn.disabled = true;
        if (shortDescEn) shortDescEn.disabled = true;
    }

    hideModal() {
        const modal = document.getElementById('categoryModal');
        if (!modal) return;

        // Remove show class first to trigger animation
        modal.classList.remove('show');
        // Hide modal after animation completes
        setTimeout(() => {
            modal.style.display = 'none';
        }, 300);
    }

    handleFormSubmit(e) {
        e.preventDefault();

        const titleEn = document.getElementById('titleEn')?.value.trim();
        const titleBo = document.getElementById('titleBo')?.value.trim();
        const descEn = document.getElementById('descEn')?.value.trim();
        const descBo = document.getElementById('descBo')?.value.trim();
        const shortDescEn = document.getElementById('shortDescEn')?.value.trim();
        const shortdescBo = document.getElementById('shortdescBo')?.value.trim();

        if (!titleEn || !titleBo) {
            alert('Both English and Tibetan titles are required');
            return;
        }

        if ((descEn && !descBo) || (shortDescEn && !shortdescBo)) {
            alert('English descriptions can only exist if Tibetan descriptions are provided');
            return;
        }

        const newNode = new CategoryNode(titleEn, titleBo, descEn, descBo, shortDescEn, shortdescBo);

        if (!this.root) {
            this.root = newNode;
        } else if (this.selectedNode) {
            this.selectedNode.addChild(newNode);
        } else {
            alert('Please select a parent node first');
            return;
        }

        this.hideModal();
        this.renderTree();
    }

    toggleLanguage() {
        this.currentLanguage = this.currentLanguage === 'en' ? 'ti' : 'en';
        const langText = document.querySelector('.lang-text');
        if (langText) {
            langText.textContent = this.currentLanguage.toUpperCase();
        }
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
            toggleIcon: document.querySelector('.toggle-icon')
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
        if (!selectedPecha) return;

        const pechaId = selectedPecha.dataset.value;
        if (!pechaId) {
            this.showInitialMetadataState();
            return;
        }

        try {
            this.showLoadingState();
            const metadata = await this.fetchMetadata(pechaId);
            this.displayMetadata(metadata);
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
