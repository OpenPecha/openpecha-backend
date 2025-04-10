const loadConfig = async () => {
    try {
        // Determine which environment we're in based on the current hostname
        const hostname = window.location.hostname;
        
        // For localhost, try to load from config.json first
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
        
        // For deployed environments, fetch the Firebase init.json to get the projectId
        try {
            const firebaseConfigResponse = await fetch('/__/firebase/init.json', {
                headers: {
                    'Accept': 'application/json',
                    'Cache-Control': 'no-cache'
                },
                cache: 'no-store'
            });
            
            if (firebaseConfigResponse.ok) {
                const firebaseConfig = await firebaseConfigResponse.json();
                const projectId = firebaseConfig.projectId;
                
                console.log(`Detected Firebase project ID: ${projectId}`);
                
                // Map project IDs to API endpoints
                if (projectId === 'pecha-backend') {
                    console.log("Using production environment based on project ID");
                    return 'https://api-aq25662yyq-uc.a.run.app';
                } else if (projectId === 'pecha-backend-dev') {
                    console.log("Using development environment based on project ID");
                    return 'https://api-l25bgmwqoa-uc.a.run.app';
                }
            }
            
            // Fallback to hostname-based detection if Firebase config doesn't work
            console.log("Falling back to hostname-based detection");
            if (hostname === 'pecha-backend.web.app' || hostname === 'pecha-backend.firebaseapp.com') {
                console.log("Using production environment based on hostname");
                return 'https://api-aq25662yyq-uc.a.run.app';
            } else if (hostname === 'pecha-backend-dev.web.app' || hostname === 'pecha-backend-dev.firebaseapp.com') {
                console.log("Using development environment based on hostname");
                return 'https://api-l25bgmwqoa-uc.a.run.app';
            }
            
            // For unknown domains/projects, default to dev environment
            console.log("Unknown domain/project, defaulting to development environment");
            return 'https://api-l25bgmwqoa-uc.a.run.app';
            
        } catch (firebaseError) {
            console.warn("Failed to fetch Firebase config:", firebaseError);
            
            // Fallback to hostname-based detection
            console.log("Falling back to hostname-based detection after Firebase config error");
            if (hostname === 'pecha-backend.web.app' || hostname === 'pecha-backend.firebaseapp.com') {
                return 'https://api-aq25662yyq-uc.a.run.app';
            } else if (hostname === 'pecha-backend-dev.web.app' || hostname === 'pecha-backend-dev.firebaseapp.com') {
                return 'https://api-l25bgmwqoa-uc.a.run.app';
            }
            
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