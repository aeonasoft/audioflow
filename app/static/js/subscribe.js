document.getElementById('subscribe-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const langPreference = document.getElementById('lang_preference').value;
    const name = document.getElementById('name').value;
    const emailAddress = document.getElementById('email_address').value;

    const alertBox = document.getElementById('alert-box');
    alertBox.classList.add('hidden');

    try {
        const response = await fetch('/subscribe', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                name: name,
                lang_preference: langPreference,
                email_address: emailAddress,
            }),
        });

        const result = await response.json();

        if (result.status === 'success') {
            alertBox.className = 'alert success';
        } else {
            alertBox.className = 'alert error';
        }

        alertBox.innerText = result.message;
        alertBox.classList.remove('hidden');

        setTimeout(() => {
            alertBox.classList.add('hidden');
        }, 10000);

    } catch (error) {
        alertBox.className = 'alert error';
        alertBox.innerText = '😢 Sorry, something went wrong. Please try again later.';
        alertBox.classList.remove('hidden');

        setTimeout(() => {
            alertBox.classList.add('hidden');
        }, 10000);
    }
});
