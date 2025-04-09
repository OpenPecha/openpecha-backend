const loadConfig = async () => {
    try {
        // Determine which environment we're in based on the current hostname
        const hostname = window.location.hostname;
        
        // For localhost, try to load from config.json
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
            console.log("Running on localhost, attempting to load local config...");
            try {
                const localResponse = await fetch('/config.json', {
                    headers: {
                        'Accept': 'application/json',
                        'Cache-Control': 'no-cache'
                    },
                    cache: 'no-store'
                });
                
                if (localResponse.ok) {
                    const localConfig = await localResponse.json();
                    if (localConfig.apiEndpoint) {
                        console.log("Using local config from config.json");
                        return localConfig.apiEndpoint.replace(/\/$/, ''); // Remove trailing slash if present
                    }
                }
                
                // If local config doesn't have apiEndpoint, fall back to dev endpoint
                console.log("Local config.json found but missing apiEndpoint, using dev endpoint");
                return 'https://api-l25bgmwqoa-uc.a.run.app';
            } catch (localError) {
                console.warn("Failed to load local config.json:", localError);
                console.log("Using development endpoint for localhost");
                return 'https://api-l25bgmwqoa-uc.a.run.app';
            }
        }
        
        // For deployed environments, directly return the appropriate endpoint
        if (hostname === 'pecha-backend.web.app' || hostname === 'pecha-backend.firebaseapp.com') {
            console.log("Using production environment");
            return 'https://api-aq25662yyq-uc.a.run.app';
        } else if (hostname === 'pecha-backend-dev.web.app' || hostname === 'pecha-backend-dev.firebaseapp.com') {
            console.log("Using development environment");
            return 'https://api-l25bgmwqoa-uc.a.run.app';
        } else {
            // For unknown domains, default to dev environment
            console.log("Unknown domain, defaulting to development environment");
            return 'https://api-l25bgmwqoa-uc.a.run.app';
        }
    } catch (error) {
        console.error('Config error:', error);
        // Use development endpoint as fallback
        console.warn('Using development endpoint as fallback');
        return 'https://api-l25bgmwqoa-uc.a.run.app';
    }
};