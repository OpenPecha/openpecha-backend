class HistoricalDatePicker {
    constructor() {
        this.currentDate = new Date();
        this.selectedDate = null;
        this.era = 'Standard';
        this.historicalStart = null;
        this.historicalEnd = null;
        this.initializeElements();
        this.addEventListeners();
        this.renderCalendar();
    }

    initializeElements() {
        this.datePickerContainer = document.getElementById('datePickerContainer');
        this.eraSelect = document.getElementById('eraSelect');
        this.prevYearBtn = document.getElementById('prevYear');
        this.nextYearBtn = document.getElementById('nextYear');
        this.prevMonthBtn = document.getElementById('prevMonth');
        this.nextMonthBtn = document.getElementById('nextMonth');
        this.currentMonthElement = document.getElementById('currentMonth');
        this.yearSelectInput = document.getElementById("yearInput");
        this.calendarGrid = document.getElementById('calendarGrid');
        this.selectedDateElement = document.getElementById('selectedDate');
        this.historicalYearStart = document.getElementById('historicalYearStart');
        this.historicalYearEnd = document.getElementById('historicalYearEnd');
        this.standardPicker = document.getElementById('standardPicker');
        this.historicalPicker = document.getElementById('historicalPicker');
    }

    addEventListeners() {
        this.selectedDateElement.addEventListener('click', event => {
            this.datePickerContainer.classList.toggle('open');
        });
        this.prevYearBtn.addEventListener('click', () => this.changeYear(-1));
        this.nextYearBtn.addEventListener('click', () => this.changeYear(1));
        this.prevMonthBtn.addEventListener('click', () => this.changeMonth(-1));
        this.nextMonthBtn.addEventListener('click', () => this.changeMonth(1));

        // Add year input event listener
        this.yearSelectInput.addEventListener('change', () => {
            const year = parseInt(this.yearSelectInput.value);
            if (!isNaN(year) && year > 0 && year < 10000) {
                this.currentDate.setFullYear(year);
                this.renderCalendar();
            } else {
                // Reset to current value if invalid
                this.yearSelectInput.value = this.currentDate.getFullYear();
            }
        });

        // Prevent calendar from closing when clicking on year input
        this.yearSelectInput.addEventListener('click', (event) => {
            event.stopPropagation();
        });

        this.eraSelect.addEventListener('change', () => {
            this.era = this.eraSelect.value;
            this.updatePickerVisibility();
            this.updateDateDisplay();
        });

        this.historicalYearStart.addEventListener('change', (event) => {
            const year = parseInt(this.historicalYearStart.value);
            this.historicalStart = !isNaN(year) ? year : 0;
            this.updateDateDisplay();
            this.datePickerContainer.classList.remove('open');
        });
        this.historicalYearEnd.addEventListener('change', (event) => {
            const year = parseInt(this.historicalYearEnd.value);
            this.historicalEnd = !isNaN(year) ? year : 0;
            this.updateDateDisplay();
            this.datePickerContainer.classList.remove('open');
        });
    }

    updatePickerVisibility() {
        if (this.era === 'Standard') {
            this.standardPicker.classList.remove('hidden');
            this.historicalPicker.classList.add('hidden');
        } else {
            this.standardPicker.classList.add('hidden');
            this.historicalPicker.classList.remove('hidden');
        }
    }

    changeYear(delta) {
        this.currentDate.setFullYear(this.currentDate.getFullYear() + delta);
        this.renderCalendar();
    }

    changeMonth(delta) {
        this.currentDate.setMonth(this.currentDate.getMonth() + delta);
        this.renderCalendar();
    }

    renderCalendar() {
        this.calendarGrid.innerHTML = '';

        // Add weekday headers
        const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        weekdays.forEach(day => {
            const cell = document.createElement('div');
            cell.className = 'calendar-cell weekday-header';
            cell.textContent = day;
            this.calendarGrid.appendChild(cell);
        });

        const year = this.currentDate.getFullYear();
        const month = this.currentDate.getMonth();

        // Update month/year display
        const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
            'July', 'August', 'September', 'October', 'November', 'December'];
        this.currentMonthElement.textContent = `${monthNames[month]}`;
        this.yearSelectInput.value = year;

        const firstDay = new Date(year, month, 1);
        const lastDay = new Date(year, month + 1, 0);

        // Add empty cells for days before the first day of the month
        for (let i = 0; i < firstDay.getDay(); i++) {
            const cell = document.createElement('div');
            cell.className = 'calendar-cell';
            this.calendarGrid.appendChild(cell);
        }

        // Add cells for each day of the month
        for (let day = 1; day <= lastDay.getDate(); day++) {
            const cell = document.createElement('div');
            cell.className = 'calendar-cell';
            cell.textContent = day;

            if (this.selectedDate &&
                this.selectedDate.getDate() === day &&
                this.selectedDate.getMonth() === month &&
                this.selectedDate.getFullYear() === year) {
                cell.classList.add('selected');
            }

            cell.addEventListener('click', () => this.selectDate(new Date(year, month, day)));
            this.calendarGrid.appendChild(cell);
        }
    }

    selectDate(date) {
        this.selectedDate = date;
        this.updateDateDisplay();
        this.renderCalendar();
        this.datePickerContainer.classList.remove('open');
    }

    updateDateDisplay() {
        if (this.era === 'Standard' && this.selectedDate) {
            const day = this.selectedDate.getDate();
            const month = this.selectedDate.getMonth() + 1;
            const year = this.selectedDate.getFullYear();
            this.selectedDateElement.textContent = `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
        } else if ((this.era === 'BCE' || this.era === 'CE') && this.historicalStart) {
            this.selectedDateElement.textContent =
                `${this.historicalStart}${this.historicalEnd ? "-" + this.historicalEnd : ""} ${this.era}`;
        } else {
            this.selectedDateElement.textContent = 'No date selected';
        }
    }
}

// Initialize the date picker
const datePicker = new HistoricalDatePicker();