window.onload = async function() {
  //<editor-fold desc="Changeable Configuration Block">

  try {
    // Dynamically load config.js
    await new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.src = '../config.js';
      script.onload = resolve;
      script.onerror = reject;
      document.head.appendChild(script);
    });
    
    const apiEndpoint = await getApiEndpoint();
    if (!apiEndpoint) throw new Error("Failed to get API endpoint");
    
    const schemaUrl = `${apiEndpoint}/schema/openapi`;
    console.log("Using schema URL:", schemaUrl);
    
    window.ui = SwaggerUIBundle({
      url: schemaUrl,
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
      plugins: [SwaggerUIBundle.plugins.DownloadUrl],
      layout: "StandaloneLayout"
    });
  } catch (error) {
    console.error('Error initializing Swagger UI:', error);
  }

  //</editor-fold>
};
