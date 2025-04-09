class CustomSearchableDropdown {
    constructor(selectElementContainer, options, elementId="") {
          console.log("CustomSearchableDropdown constructor", selectElementContainer, options, elementId);
        this.selectElementContainer = selectElementContainer;
        this.options = options.metadata ?? options;
        this.wrapperId = elementId;
        this.createCustomDropdown();
        this.bindEvents();
        console.log("CustomSearchableDropdown initialized", elementId);
    }

    createCustomDropdown() {
        this.dropdownWrapper = document.createElement('div');
        this.dropdownWrapper.classList.add('custom-dropdown');

        this.dropdownTrigger = document.createElement('div');
        this.dropdownTrigger.classList.add('dropdown-trigger');
        this.dropdownTrigger.dataset.value = '';
        this.dropdownTrigger.id = this.wrapperId;
        this.dropdownTrigger.textContent = "Select an option";
        // Create dropdown options container
        this.dropdownOptionsContainer = document.createElement('div');
        this.dropdownOptionsContainer.classList.add('dropdown-options-container', 'hidden');

        // Create search input
        this.searchInput = document.createElement('input');
        this.searchInput.type = 'text';
        this.searchInput.placeholder = 'Search...';
        this.searchInput.classList.add('dropdown-search-input');

        // Create options list
        this.optionsList = document.createElement('ul');
        this.optionsList.classList.add('dropdown-options');
        // Create a default option with null value
        const defaultOption = document.createElement('li');
        defaultOption.textContent = 'Select an option';
        defaultOption.dataset.value = ""; // Set the value to null
        this.optionsList.appendChild(defaultOption);
        // Populate options list
        this.options.forEach((option) => {
            if (option.title) {  
                const listItem = document.createElement('li');
                listItem.textContent = `${option.id} - ${option.title[option.language]}`;
                listItem.dataset.value = option.id;
                this.optionsList.appendChild(listItem);
            }else{
                const listItem = document.createElement('li');
                listItem.textContent = `${option.name}`;
                listItem.dataset.value = option.id;
                this.optionsList.appendChild(listItem);
            }
        });

        // Assemble dropdown
        this.dropdownOptionsContainer.appendChild(this.searchInput);
        this.dropdownOptionsContainer.appendChild(this.optionsList);
        this.dropdownWrapper.appendChild(this.dropdownTrigger);
        this.dropdownWrapper.appendChild(this.dropdownOptionsContainer);
        // Replace select element
        // this.selectElement?.parentNode.replaceChild(this.dropdownWrapper, this.selectElement);
        this.selectElementContainer.appendChild(this.dropdownWrapper);
        // Add search functionality
        this.searchInput.addEventListener('input', this.filterOptions.bind(this));
    }

    bindEvents() {
        // Toggle dropdown
        this.dropdownTrigger.addEventListener('click', () => {
            this.dropdownOptionsContainer.classList.toggle('hidden');
            this.searchInput.value = '';
            this.filterOptions(); // Reset to show all options
            this.searchInput.focus();
        });

        // Select option
        this.optionsList.addEventListener('click', (e) => {
            if (e.target.tagName === 'LI') {
                const selectedText = e.target.textContent;

                // Update trigger text
                this.dropdownTrigger.dataset.value = e.target.dataset.value;
                this.dropdownTrigger.textContent = selectedText;

                // Close dropdown
                this.dropdownOptionsContainer.classList.add('hidden');
                this.triggerChangeEvent();
            }
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!this.dropdownWrapper.contains(e.target)) {
                this.dropdownOptionsContainer.classList.add('hidden');
            }
        });
    }

    filterOptions() {
        const searchTerm = this.searchInput.value.toLowerCase().trim();

        Array.from(this.optionsList.children).forEach(option => {
            const matches = option.textContent.toLowerCase().includes(searchTerm);
            option.style.display = matches ? 'block' : 'none';
        });
    }

    triggerChangeEvent() {
        // Create and dispatch a custom event
        const changeEvent = new CustomEvent('customDropdownChange', {
            detail: {
                value: this.dropdownTrigger.dataset.value,
                text: this.dropdownTrigger.textContent
            },
            bubbles: true
        });
        this.dropdownTrigger.dispatchEvent(changeEvent);
    }
}