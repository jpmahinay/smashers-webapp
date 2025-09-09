document.addEventListener('DOMContentLoaded', function() {
    console.log("ShuttleRank App - Aqua Slate Theme Initialized");

    // --- Mobile Navigation Toggle ---
    const navToggle = document.getElementById('nav-toggle');
    const mobileNavMenu = document.getElementById('mobile-nav-menu');

    if (navToggle) {
        navToggle.addEventListener('click', () => {
            mobileNavMenu.classList.toggle('active');
            navToggle.classList.toggle('is-active');
        });
    }

    // --- Confirmation Dialogs (keeping existing functionality) ---
    document.body.addEventListener('click', function(event) {
        const target = event.target;
        
        // Start match confirmation
        if (target.matches('.btn-start')) {
            const confirmed = confirm('Are you sure you want to start this match? It cannot be canceled afterward.');
            if (!confirmed) {
                event.preventDefault();
            }
        }

        // Cancel match confirmation
        if (target.matches('.btn-cancel')) {
            const confirmed = confirm('Are you sure you want to permanently cancel this scheduled match?');
            if (!confirmed) {
                event.preventDefault();
            }
        }
    });
});