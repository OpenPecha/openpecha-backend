class PechaRelationship {
    constructor() {
        // Initialize elements
        this.elements = {
            pechaSelect: document.getElementById('pecha-select'),
            selectSpinner: document.getElementById('select-spinner'),
            loadingContainer: document.getElementById('loading-container'),
            noDataMessage: document.getElementById('no-data-message'),
            graphContainer: document.getElementById('graph-container'),
            resetZoomButton: document.getElementById('reset-zoom'),
            zoomInButton: document.getElementById('zoom-in'),
            zoomOutButton: document.getElementById('zoom-out'),
            toastContainer: document.getElementById('toastContainer'),
            traversalSelect: document.getElementById('traversal-select'),
            commentaryCheckbox: document.getElementById('commentary-checkbox'),
            versionCheckbox: document.getElementById('version-checkbox'),
            translationCheckbox: document.getElementById('translation-checkbox'),
            applyFiltersBtn: document.getElementById('apply-filters-btn'),
            // Add references to legend elements
            toggleLegendBtn: document.getElementById('toggle-legend'),
            visualizationLegend: document.getElementById('visualization-legend'),
            // Search elements
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            searchContainers: document.querySelectorAll('.select-search-container')
        };

        // Initialize state
        this.state = {
            selectedPecha: null,
            relationshipData: null,
            isLoading: false,
            graph: null,
            simulation: null,
            svg: null,
            zoom: null,
            currentZoom: null,
            width: 0,
            height: 0,
            filters: {
                traversal: 'full_tree',
                relationships: ['commentary', 'version', 'translation']
            }
        };

        // D3 color scale for relationship types
        this.relationshipColors = {
            'version_of': '#00a4e4',    // Blue (more vibrant)
            'commentary_of': '#ff5722',  // Deep orange (more distinct)
            'translation_of': '#8bc34a',  // Light green (more distinct)
            'root': getComputedStyle(document.documentElement).getPropertyValue('--selected-color').trim()
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

    // Toggle loading spinner
    toggleLoadingSpinner(show) {
        this.elements.selectSpinner.style.display = show ? 'block' : 'none';
    }

    // Toggle main loading container
    toggleLoading(show) {
        this.elements.loadingContainer.style.display = show ? 'flex' : 'none';
        this.state.isLoading = show;
    }

    // Toggle no data message
    toggleNoDataMessage(show) {
        this.elements.noDataMessage.style.display = show ? 'flex' : 'none';
    }

    // Fetch all pechas for the dropdown
    async fetchPechaList() {
        this.toggleLoadingSpinner(true);
        
        let body = {};

        try {
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

    // Populate the pecha select dropdown
    populatePechaSelect(pechas) {
        // Clear existing options except the default one
        while (this.elements.pechaSelect.options.length > 1) {
            this.elements.pechaSelect.remove(1);
        }

        // Sort pechas by ID
        pechas.sort((a, b) => a.id.localeCompare(b.id));

        // Add new options
        pechas.forEach(pecha => {
            const option = document.createElement('option');
            option.value = pecha.id;
            
            // Get title in any available language
            let title = this.getTitle(pecha);
            option.textContent = `${pecha.id} - ${title}`;
            
            this.elements.pechaSelect.appendChild(option);
        });
    }

    // Helper to get title in any available language
    getTitle(pecha) {
        if (!pecha.title) return 'Untitled';
        
        // Try to get its language title first
        if (pecha.title[pecha.language]) return pecha.title[pecha.language];
        
        // Otherwise return tibetan title
        return pecha.title.bo || 'Untitled';
    }

    findPecha(data, id) {
        return data[id] || null;
    }

    // Fetch relationship data for a specific pecha
    async fetchRelationshipData(pechaId, traversal = 'full_tree', relationships =[]) {
        this.toggleLoading(true);
        this.toggleNoDataMessage(false);
        
        try {
            const url = `${this.API_ENDPOINT}/metadata/${pechaId}/related?traversal=${traversal}${relationships.length > 0 ? `&relationships=${relationships.join(',')}` : ''}`;
            const response = await fetch(url, {
                method: 'GET',
                headers: {
                    'accept': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`Failed to fetch relationship data: ${response.statusText}`);
            }
            
            let data = await response.json();
            
            this.toggleLoading(false);
            
            // Check if data is empty
            if (!data || (Array.isArray(data) && data.length === 0) || 
                (!Array.isArray(data) && Object.keys(data).length === 0)) {
                this.toggleNoDataMessage(true);
                return null;
            }
            
            return this.processRelationshipData(data, pechaId);
        } catch (error) {
            console.error("Error fetching relationship data:", error);
            this.showToast("Unable to load relationship data. Please try again later.", 'error');
            this.toggleLoading(false);
            this.toggleNoDataMessage(true);
            return null;
        }
    }
    
    // Process the API response into a format suitable for D3 visualization
    processRelationshipData(data, selectedPechaId) {
        // Create nodes and links arrays for D3
        const nodes = [];
        const links = [];
        const nodeMap = new Map(); // To track nodes we've already added
        
        console.log("Processing data format:", data);
        
        // Check if the data is an array (the format from the sample)
        if (Array.isArray(data)) {
            // Find the root/selected pecha in the array
            const selectedNode = data.find(item => item.id === selectedPechaId);
            
            if (!selectedNode) {
                console.error("Selected pecha not found in data");
                return { nodes: [], links: [] };
            }
                // First, add all nodes to the map
                data.forEach(item => {
                    if (!nodeMap.has(item.id)) {
                        const node = {
                        id: item.id,
                        title: item.title || {},
                        group: 'root',
                        isSelected: item.id === selectedPechaId
                    };
                    
                    nodeMap.set(item.id, node);
                }
            });
            
            // Then create links based on relationships
            data.forEach(item => {
                // Handle version relationship
                const type = item.type;
                console.log("Type:", type);
                if (type === 'version') {
                    const sourceNode = nodeMap.get(item.parent);
                    const targetNode = nodeMap.get(item.id);
                    
                    if (sourceNode && targetNode) {
                        targetNode.group = 'version';
                        
                        links.push({
                            source: item.parent,
                            target: item.id,
                            type: 'version'
                        });
                    }
                }
                
                // Handle commentary relationship
                if (type === 'commentary') {
                    const sourceNode = nodeMap.get(item.parent);
                    const targetNode = nodeMap.get(item.id);
                    
                    if (sourceNode && targetNode) {
                        targetNode.group = 'commentary';
                        
                        links.push({
                            source: item.parent,
                            target: item.id,
                            type: 'commentary'
                        });
                    }
                }
                
                // Handle translation relationship
                if (type === 'translation') {
                    const sourceNode = nodeMap.get(item.parent);
                    const targetNode = nodeMap.get(item.id);
                    
                    if (sourceNode && targetNode) {
                        targetNode.group = 'translation';
                        
                        links.push({
                            source: item.parent,
                            target: item.id,
                            type: 'translation'
                        });
                    }
                }
            });
            // Convert the node map to an array for D3
            return { 
                nodes: Array.from(nodeMap.values()), 
                links: links 
            };
        } else {
            console.log("Invalid data format", data);
            return null;
        }
    }

    // Initialize the D3 graph visualization
    initializeGraph() {
        // Clear any existing graph
        d3.select(this.elements.graphContainer).selectAll('*').remove();
        
        // Get container dimensions
        const containerRect = this.elements.graphContainer.getBoundingClientRect();
        this.state.width = containerRect.width;
        this.state.height = containerRect.height;
        
        // Create SVG element
        const svg = d3.select(this.elements.graphContainer)
            .append('svg')
            .attr('width', this.state.width)
            .attr('height', this.state.height);
            
        // Create a group for the graph that will be transformed by zoom
        const g = svg.append('g');
        
        // Initialize zoom behavior
        this.state.zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
                this.state.currentZoom = event.transform;
            });
            
        // Apply zoom behavior to the SVG
        svg.call(this.state.zoom);
        
        // Create tooltip div
        const tooltip = d3.select('body')
            .append('div')
            .attr('class', 'node-tooltip')
            .style('opacity', 0);
            
        // Store references
        this.state.svg = svg;
        this.state.g = g;
        this.state.tooltip = tooltip;
        
        // Initial zoom to fit
        this.resetZoom();
        
        return { svg, g, tooltip };
    }

    // Render the graph with the provided data
    renderGraph(data) {
        if (!data) return;
        console.log(JSON.stringify(data, null, 2));
        console.log('Rendering graph with nodes:', data.nodes.length, 'links:', data.links.length);
        
        const { svg, g, tooltip } = this.initializeGraph();
        
        // Find the root pecha node
        const rootNode = data.nodes.find(n => n.group === 'root');
        if (rootNode) {
            // Pin the root node to the top center
            rootNode.fx = this.state.width / 2;
            rootNode.fy = this.state.height / 5; // Position at 1/5 from the top for better visibility
            console.log('Pinned root node:', rootNode.id);
        }
        
        // Create force simulation
        const simulation = d3.forceSimulation(data.nodes)
            .force('link', d3.forceLink(data.links).id(d => d.id).distance(220)) // Increased distance for better visibility
            .force('charge', d3.forceManyBody().strength(-1000)) // Stronger repulsion
            .force('center', d3.forceCenter(this.state.width / 2, this.state.height / 2))
            .force('collision', d3.forceCollide().radius(130)) // Larger collision radius
            .force('y', d3.forceY().strength(0.08)); // Slightly stronger vertical force
            
        // Create arrow marker definitions for the links
        svg.append('defs').selectAll('marker')
            .data(['version', 'commentary', 'translation', 'root'])
            .enter().append('marker')
            .attr('id', d => `arrow-${d}`)
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 30) // Position the arrow away from the node
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', d => {
                console.log("d",d)
                if (d === 'version') return this.relationshipColors.version_of;
                if (d === 'commentary') return this.relationshipColors.commentary_of;
                if (d === 'translation') return this.relationshipColors.translation_of;
                if (d === 'root') return this.relationshipColors.root;
                return '#555';
            });
            
        // Create links - using paths with arrows instead of simple lines
        const link = g.append('g')
            .attr('class', 'links')
            .selectAll('path')
            .data(data.links)
            .enter().append('path')
            .attr('class', d => `link link-${d.type}`)
            .attr('marker-end', d => `url(#arrow-${d.type.split('_')[0]})`);
            
        // Create nodes
        const node = g.append('g')
            .attr('class', 'nodes')
            .selectAll('.node')
            .data(data.nodes)
            .enter().append('g')
            .attr('class', 'node')
            .call(d3.drag()
                .on('start', this.dragstarted.bind(this, simulation))
                .on('drag', this.dragged.bind(this))
                .on('end', this.dragended.bind(this, simulation)));
        
        // Get title for a node
        const getNodeTitle = (d) => {
            if (d.title) {
                if (d.title.en) return d.title.en;
                else if (Object.keys(d.title).length > 0) {
                    const lang = Object.keys(d.title)[0];
                    return d.title[lang];
                }
            }
            return 'Untitled';
        };
        
        // Calculate node dimensions based on text length
        const calculateNodeDimensions = (d) => {
            const idLength = d.id.length;
            const titleText = d.title;
            const titleLength = titleText.length;
            // Ensure the width is at least enough to show the full ID
            const width = Math.max(idLength * 9, titleLength * 6, 150);
            return {
                width: width,
                height: 50,
                title: titleText
            };
        };
        
        // Add rectangles to nodes
        node.each(function(d) {
            const dimensions = calculateNodeDimensions(d);
            d.width = dimensions.width;
            d.height = dimensions.height;
            d.fullTitle = dimensions.title;
            
            // Create the main rectangle for the node
            d3.select(this).append('rect')
                .attr('class', `node-rect ${d.group}`)
                .attr('width', d.width)
                .attr('height', d.height)
                .attr('rx', 10)
                .attr('ry', 10)
                .attr('x', -d.width / 2)
                .attr('y', -d.height / 2);
                
            // Add an additional outline for selected nodes to make them more visible
            if (d.group === 'selected' || d.group === 'root') {
                d3.select(this).append('rect')
                    .attr('class', `node-selected-outline`)
                    .attr('width', d.width + 4) // Slightly larger than the main rectangle
                    .attr('height', d.height + 4)
                    .attr('rx', 12)
                    .attr('ry', 12)
                    .attr('x', -(d.width + 4) / 2)
                    .attr('y', -(d.height + 4) / 2);
            }
        });
        
        // Add ID text labels to nodes
        node.append('text')
            .attr('class', 'node-id')
            .attr('dy', -5)
            .text(d => d.id)
            .attr('text-anchor', 'middle');
        
        // Add title text labels to nodes with ellipsis if needed
        node.append('text')
            .attr('class', 'node-title')
            .attr('dy', 10)
            .text(d => {
                const title = d.fullTitle;
                const maxLength = Math.floor(d.width / 6);
                if (title.length > maxLength) {
                    return title.substring(0, maxLength - 3) + '...';
                }
                return title;
            })
            .attr('text-anchor', 'middle');
        
        // Add click-to-copy functionality
        node.on('click', (event, d) => {
            // Create a temporary textarea element to copy the text
            const textarea = document.createElement('textarea');
            // Copy both ID and title
            const title = d.fullTitle || 'Untitled';
            textarea.value = `${d.id} - ${title}`;
            document.body.appendChild(textarea);
            textarea.select();
            
            try {
                // Execute the copy command
                document.execCommand('copy');
                
                // Show a toast notification
                this.showToast(`Copied: ${d.id} - ${title}`, 'success');
            } catch (err) {
                console.error('Failed to copy text: ', err);
                this.showToast('Failed to copy pecha information', 'error');
            } finally {
                // Clean up
                document.body.removeChild(textarea);
            }
            
            // Prevent event propagation
            event.stopPropagation();
        });
        
        // Add hover effect for better UX
        node.on('mouseover', (event, d) => {
            // Change cursor to indicate clickable element
            d3.select(event.currentTarget).style('cursor', 'pointer');
        });
        
        // Update positions on each tick of the simulation
        simulation.on('tick', () => {
            // Create curved paths for links
            link.attr('d', function(d) {
                const dx = d.target.x - d.source.x;
                const dy = d.target.y - d.source.y;
                const dr = Math.sqrt(dx * dx + dy * dy);
                
                // Return a curved path
                return `M${d.source.x},${d.source.y}A${dr},${dr} 0 0,1 ${d.target.x},${d.target.y}`;
            });
            
            node
                .attr('transform', d => `translate(${d.x},${d.y})`);
        });
        
        // Store simulation reference
        this.state.simulation = simulation;
    }

    // Determine node color based on its relationships
    getNodeColor(node, links) {
        // Default color
        let color = '#aaa';
        
        // Check if this node has any relationships
        const nodeLinks = links.filter(link => 
            link.source.id === node.id || link.target.id === node.id
        );
        
        if (nodeLinks.length > 0) {
            // Use the color of the first relationship type found
            const firstLink = nodeLinks[0];
            color = this.relationshipColors[firstLink.type] || color;
        }
        
        return color;
    }

    // Handle node dragging events
    dragstarted(simulation, event, d) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }

    dragended(simulation, event, d) {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }

    // Reset zoom to fit the graph
    resetZoom() {
        if (!this.state.svg || !this.state.g) return;
        
        const svg = this.state.svg;
        const g = this.state.g;
        
        // Reset the transform
        svg.transition().duration(750).call(
            this.state.zoom.transform,
            d3.zoomIdentity.translate(this.state.width / 2, this.state.height / 2).scale(0.8)
        );
    }

    // Zoom in
    zoomIn() {
        if (!this.state.svg) return;
        
        this.state.svg.transition().duration(300).call(
            this.state.zoom.scaleBy,
            1.2
        );
    }

    // Zoom out
    zoomOut() {
        if (!this.state.svg) return;
        
        this.state.svg.transition().duration(300).call(
            this.state.zoom.scaleBy,
            0.8
        );
    }

    // Handle pecha selection change
    async handlePechaChange(applyFilters = false) {
        const selectedPechaId = this.elements.pechaSelect.value;
        
        if (!selectedPechaId) {
            // Clear the graph if no pecha is selected
            d3.select(this.elements.graphContainer).selectAll('*').remove();
            this.toggleNoDataMessage(false);
            return;
        }
        
        this.state.selectedPecha = selectedPechaId;
        
        // Only update filters if explicitly requested (via Apply Filters button)
        if (applyFilters) {
            this.updateFilterState();
        }
        
        // Use the current filter state
        const traversal = this.state.filters.traversal;
        const relationships = this.state.filters.relationships;
        
        // Show a toast notification when filters are applied
        if (applyFilters) {
            const relationshipText = relationships.length > 0 ? 
                relationships.join(', ') : 'none';
            this.showToast(`Applying filters: Traversal: ${traversal}, Relationships: ${relationshipText}`, 'info');
        }
        
        // Fetch and render relationship data with traversal and relationship filters
        const relationshipData = await this.fetchRelationshipData(selectedPechaId, traversal, relationships);
        this.state.relationshipData = relationshipData;
        if (relationshipData) {
            this.renderGraph(relationshipData);
        }
    }
    
    // Update filter state from UI controls
    updateFilterState() {
        // Update traversal type
        this.state.filters.traversal = this.elements.traversalSelect?.value || 'full_tree';
        
        // Update relationship types
        const relationships = [];
        if (this.elements.commentaryCheckbox?.checked) relationships.push('commentary');
        if (this.elements.versionCheckbox?.checked) relationships.push('version');
        if (this.elements.translationCheckbox?.checked) relationships.push('translation');
        
        this.state.filters.relationships = relationships;
    }

    // Setup event listeners
    setupEventListeners() {
        // Pecha select change
        this.elements.pechaSelect.addEventListener('change', () => {
            // When pecha changes, we want to immediately apply current filters
            this.updateFilterState();
            this.handlePechaChange(false);
        });
        
        // Reset zoom button
        this.elements.resetZoomButton.addEventListener('click', () => this.resetZoom());
        
        // Zoom buttons
        this.elements.zoomInButton.addEventListener('click', () => this.zoomIn());
        this.elements.zoomOutButton.addEventListener('click', () => this.zoomOut());
        
        // Apply filters button - explicit filter application
        this.elements.applyFiltersBtn?.addEventListener('click', () => this.handlePechaChange(true));
        
        // Toggle legend button
        this.elements.toggleLegendBtn?.addEventListener('click', () => this.toggleLegend());
        
        // Filter change events - we'll update the state but not trigger a fetch
        // until the Apply Filters button is clicked
        const filterControls = [
            this.elements.traversalSelect,
            this.elements.commentaryCheckbox,
            this.elements.versionCheckbox,
            this.elements.translationCheckbox
        ];
        
        filterControls.forEach(control => {
            if (control) {
                control.addEventListener('change', () => {
                    // Update the filter state but don't apply yet
                    this.updateFilterState();
                    
                    // Highlight the apply button to indicate changes are pending
                    if (this.elements.applyFiltersBtn) {
                        this.elements.applyFiltersBtn.classList.add('highlight');
                        setTimeout(() => {
                            this.elements.applyFiltersBtn.classList.remove('highlight');
                        }, 300);
                    }
                });
            }
        });
        
        // Handle window resize
        window.addEventListener('resize', this.debounce(() => {
            if (this.state.relationshipData) {
                this.renderGraph(this.state.relationshipData);
            }
        }, 250));
    }
    
    // Toggle legend visibility
    toggleLegend() {
        if (this.elements.visualizationLegend) {
            this.elements.visualizationLegend.classList.toggle('hidden');
            
            // Update button icon based on legend visibility
            const isHidden = this.elements.visualizationLegend.classList.contains('hidden');
            if (this.elements.toggleLegendBtn) {
                this.elements.toggleLegendBtn.innerHTML = isHidden ? 
                    '<i class="fas fa-info-circle"></i>' : 
                    '<i class="fas fa-times"></i>';
                
                this.elements.toggleLegendBtn.title = isHidden ? 'Show Legend' : 'Hide Legend';
            }
        }
    }

    // Debounce function for resize handling
    debounce(func, wait) {
        let timeout;
        return function() {
            const context = this;
            const args = arguments;
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(context, args), wait);
        };
    }

    // Initialize search UI functionality
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

    // Populate search results based on search term
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

    // Navigate through search results with keyboard
    navigateSearchResults(resultsContainer, direction) {
        const items = resultsContainer.querySelectorAll('.search-item');
        const selectedItem = resultsContainer.querySelector('.search-item.selected');
        let index = -1;

        if (selectedItem) {
            for (let i = 0; i < items.length; i++) {
                if (items[i] === selectedItem) {
                    index = i;
                    break;
                }
            }
        }

        if (direction === 'down') {
            index = (index + 1) % items.length;
        } else if (direction === 'up') {
            index = (index - 1 + items.length) % items.length;
        }

        if (index >= 0 && items.length > 0) {
            items.forEach(item => item.classList.remove('selected'));
            items[index].classList.add('selected');
            items[index].scrollIntoView({ block: 'nearest' });
        }
    }

    // Initialize the application
    async init() {
        try {
            // Load API endpoint from config
            this.API_ENDPOINT = await getApiEndpoint();
            
            // Initialize filter state from UI controls
            this.updateFilterState();
            
            // Fetch pecha list and populate dropdown
            const pechas = await this.fetchPechaList();
            this.populatePechaSelect(pechas);
            
            // Initialize search UI
            this.initializeSearchUI();
            
            // Setup event listeners
            this.setupEventListeners();
            
            // Hide loading spinner
            this.toggleLoading(false);
            
        } catch (error) {
            console.error("Error initializing application:", error);
            this.showToast("Failed to initialize application. Please refresh the page.", 'error');
            this.toggleLoading(false);
        }
    }
}

// Initialize the application when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new PechaRelationship();
});
