// Reusable server check for button actions
async function checkServerAndProceed(action) {
    try {
        const response = await fetch('http://localhost:5000/health');
        if (!response.ok) throw new Error('Server not OK');
        action(); // Only run the action if server is available
    } catch (e) {
        alert("Server is NOT available. Please start the backend and try again.");
    }
}

// Example usage for a button with id 'startStream'
document.getElementById('startStream').onclick = function () {
    checkServerAndProceed(() => {
        // Your stream logic here
        alert('Stream started!');
    });
};

// Example usage for a button with id 'sendQuery'
document.getElementById('sendQuery').onclick = function () {
    checkServerAndProceed(() => {
        // Your query logic here
        alert('Query sent!');
    });
};
