package org.example;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.util.Locale;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JProgressBar;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.border.TitledBorder;

public class MigrationAppUI extends JFrame {

    // --- Các thành phần UI cần truy cập toàn cục ---
    // Source DB
    private JComboBox<String> sourceDbTypeCombo;
    private JTextField sourceHostField, sourcePortField, sourceDbNameField, sourceUserField;
    private JPasswordField sourcePassField;

    // Target DB
    private JComboBox<String> targetDbTypeCombo;
    private JTextField targetHostField, targetPortField, targetDbNameField, targetUserField;
    private JPasswordField targetPassField;

    // Options
    private JRadioButton optCopyAll, optStructureOnly, optDataOnly;
    private JCheckBox chkTruncateTarget, chkCopyNewOnly;
    private JTextField limitDataField;

    // Actions
    private JButton btnStartMigration;
    private JTextArea logArea;

    private JProgressBar progressBar;

    public MigrationAppUI() {
        setTitle("Database Migration Tool");
        setSize(850, 650);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null); // Hiển thị ở giữa màn hình
        setLayout(new BorderLayout(10, 10)); // Khoảng cách giữa các vùng là 10px

        initComponents();
    }

    private void initComponents() {
        // 1. Khu vực Cấu hình DB (Bố trí ở phía Bắc)
        JPanel dbConfigPanel = new JPanel(new GridLayout(1, 2, 10, 0));
        dbConfigPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        JPanel sourcePanel = createDbConfigPanel("Source Database (Nguồn)", true);
        JPanel targetPanel = createDbConfigPanel("Target Database (Đích)", false);

        dbConfigPanel.add(sourcePanel);
        dbConfigPanel.add(targetPanel);
        add(dbConfigPanel, BorderLayout.NORTH);

        // 2. Khu vực Tùy chọn (Bố trí ở Giữa)
        JPanel optionsPanel = createOptionsPanel();
        add(optionsPanel, BorderLayout.CENTER);

        // 3. Khu vực Log và Nút Action (Bố trí ở phía Nam)
        JPanel bottomPanel = createBottomPanel();
        add(bottomPanel, BorderLayout.SOUTH);
    }

    /**
     * Hàm dùng chung để tạo Panel cấu hình kết nối.
     * @param title Tiêu đề của Panel
     * @param isSource Xác định đây là form Source hay Target để map đúng biến
     */
    private JPanel createDbConfigPanel(String title, boolean isSource) {
        JPanel panel = new JPanel(new BorderLayout(5, 5));
        panel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(), title, TitledBorder.LEFT, TitledBorder.TOP));

        // Form nhập liệu
        JPanel formPanel = new JPanel(new GridLayout(6, 2, 5, 5));
        formPanel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

        JComboBox<String> dbTypeCombo = new JComboBox<>(new String[]{"Oracle", "PostgreSQL"});
        JTextField hostField = new JTextField(isSource ? "localhost" : "127.0.0.1");
        JTextField portField = new JTextField(isSource ? "1521" : "5432");
        JTextField dbNameField = new JTextField(isSource ? "ORCL" : "migration_db");
        JTextField userField = new JTextField(isSource ? "scott" : "postgres");
        JPasswordField passField = new JPasswordField();
        if (isSource) {
            passField.setText("tiger");
        }

        // Lưu tham chiếu vào biến toàn cục tương ứng
        if (isSource) {
            this.sourceDbTypeCombo = dbTypeCombo; this.sourceHostField = hostField;
            this.sourcePortField = portField; this.sourceDbNameField = dbNameField;
            this.sourceUserField = userField; this.sourcePassField = passField;
        } else {
            this.targetDbTypeCombo = dbTypeCombo; this.targetHostField = hostField;
            this.targetPortField = portField; this.targetDbNameField = dbNameField;
            this.targetUserField = userField; this.targetPassField = passField;
        }

        formPanel.add(new JLabel("Database Type:")); formPanel.add(dbTypeCombo);
        formPanel.add(new JLabel("Host:")); formPanel.add(hostField);
        formPanel.add(new JLabel("Port:")); formPanel.add(portField);
        formPanel.add(new JLabel("DB Name / SID:")); formPanel.add(dbNameField);
        formPanel.add(new JLabel("Username:")); formPanel.add(userField);
        formPanel.add(new JLabel("Password:")); formPanel.add(passField);

        panel.add(formPanel, BorderLayout.CENTER);

        // Nút Test Connection
        JButton btnTest = new JButton("Test Connection");
        btnTest.addActionListener(e -> testConnectionAction(isSource));
        JPanel btnPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        btnPanel.add(btnTest);
        panel.add(btnPanel, BorderLayout.SOUTH);

        return panel;
    }

    public void enableStartButton(boolean enable) {
        btnStartMigration.setEnabled(enable);
    }

    /**
     * Tạo Panel chứa các tùy chọn Migration
     */
    private JPanel createOptionsPanel() {
        JPanel panel = new JPanel(new GridBagLayout()); // Dùng GridBagLayout cho linh hoạt
        panel.setBorder(BorderFactory.createTitledBorder("Migration Options"));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(5, 10, 5, 10);

        // Nhóm RadioButton cho chế độ chính (Chỉ chọn 1)
        optCopyAll = new JRadioButton("Copy cả Cấu trúc và Dữ liệu", true);
        optStructureOnly = new JRadioButton("Chỉ copy Cấu trúc (DDL)");
        optDataOnly = new JRadioButton("Chỉ copy Dữ liệu (DML)");
        ButtonGroup modeGroup = new ButtonGroup();
        modeGroup.add(optCopyAll); modeGroup.add(optStructureOnly); modeGroup.add(optDataOnly);

        // Checkbox cho các tùy chọn phụ
        chkTruncateTarget = new JCheckBox("Xóa dữ liệu cũ ở Target trước khi copy (TRUNCATE)");
        chkCopyNewOnly = new JCheckBox("Chỉ copy dữ liệu mới (Dựa vào PK - Tính năng nâng cao)");

        // Input giới hạn dữ liệu
        JPanel limitPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        limitPanel.add(new JLabel("Giới hạn số dòng copy (Limit): "));
        limitDataField = new JTextField("0", 10);
        limitDataField.setToolTipText("Nhập 0 để copy toàn bộ");
        limitPanel.add(limitDataField);

        // Bố trí vào Panel
        gbc.gridx = 0; gbc.gridy = 0; panel.add(optCopyAll, gbc);
        gbc.gridx = 1; gbc.gridy = 0; panel.add(chkTruncateTarget, gbc);

        gbc.gridx = 0; gbc.gridy = 1; panel.add(optStructureOnly, gbc);
        gbc.gridx = 1; gbc.gridy = 1; panel.add(chkCopyNewOnly, gbc);

        gbc.gridx = 0; gbc.gridy = 2; panel.add(optDataOnly, gbc);
        gbc.gridx = 1; gbc.gridy = 2; panel.add(limitPanel, gbc);

        return panel;
    }

    /**
     * Tạo Panel chứa Log và Nút bắt đầu
     */
    private JPanel createBottomPanel() {
        JPanel panel = new JPanel(new BorderLayout(5, 5));
        panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        // Nút Start
        btnStartMigration = new JButton("START MIGRATION");
        btnStartMigration.setFont(new Font("Arial", Font.BOLD, 16));
        btnStartMigration.setBackground(new Color(46, 204, 113));
        btnStartMigration.setForeground(Color.WHITE);
        btnStartMigration.setPreferredSize(new Dimension(200, 50));
        btnStartMigration.addActionListener(e -> startMigrationAction());

        JPanel btnWrapper = new JPanel(new FlowLayout(FlowLayout.CENTER));
        btnWrapper.add(btnStartMigration);

        // --- THÊM MỚI: JProgressBar ---
        progressBar = new JProgressBar(0, 100); // Từ 0% đến 100%
        progressBar.setStringPainted(true); // Hiển thị con số % text
        progressBar.setValue(0);

        // Gộp Nút và Progress Bar vào khu vực phía Bắc của Bottom Panel
        JPanel northBottomPanel = new JPanel(new BorderLayout(0, 5));
        northBottomPanel.add(btnWrapper, BorderLayout.NORTH);
        northBottomPanel.add(progressBar, BorderLayout.SOUTH);

        panel.add(northBottomPanel, BorderLayout.NORTH);

        // Khu vực Log
        logArea = new JTextArea(8, 50);
        logArea.setEditable(false);
        logArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        JScrollPane scrollPane = new JScrollPane(logArea);
        scrollPane.setBorder(BorderFactory.createTitledBorder("Execution Logs"));
        panel.add(scrollPane, BorderLayout.CENTER);

        return panel;
    }

    // --- CÁC HÀM XỬ LÝ SỰ KIỆN (EVENT HANDLERS) ---

    private void testConnectionAction(boolean isSource) {
        String typeLabel = isSource ? "SOURCE" : "TARGET";
        ConnectionManager manager = ConnectionManager.getInstance();
        String poolId = "UI_TEST_" + typeLabel + "_" + System.nanoTime();

        try {
            DatabaseConfig config = buildDatabaseConfig(isSource);
            appendLog("Đang kiểm tra kết nối " + typeLabel + " (" + config.getType() + ")...");
            manager.createPool(poolId, config);

            if (manager.testConnection(poolId)) {
                appendLog("[SUCCESS] Kết nối " + typeLabel + " thành công!\n");
                return;
            }

            appendLog("[FAILED] Không thể kết nối " + typeLabel + ".\n");
        } catch (RuntimeException e) {
            appendLog("[FAILED] Lỗi kết nối " + typeLabel + ": " + e.getMessage() + "\n");
        } finally {
            manager.closePool(poolId);
        }
    }

    private void startMigrationAction() {
        DatabaseConfig sourceConfig;
        DatabaseConfig targetConfig;

        try {
            sourceConfig = buildDatabaseConfig(true);
            targetConfig = buildDatabaseConfig(false);
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Lỗi cấu hình", JOptionPane.ERROR_MESSAGE);
            return;
        }

        // 1. Khóa nút để tránh bấm nhiều lần tạo ra nhiều luồng
        enableStartButton(false);
        progressBar.setValue(0);
        logArea.setText(""); // Xóa log cũ

        appendLog("Khởi tạo luồng nền (Background Worker)...");

        boolean isStructureOnly = optStructureOnly.isSelected();
        boolean isDataOnly = optDataOnly.isSelected();
        boolean truncateTarget = chkTruncateTarget.isSelected();
        boolean copyNewOnly = chkCopyNewOnly.isSelected();
        Integer limitRows;

        try {
            limitRows = parseLimitRows();
        } catch (IllegalArgumentException e) {
            enableStartButton(true);
            JOptionPane.showMessageDialog(this, e.getMessage(), "Lỗi cấu hình", JOptionPane.ERROR_MESSAGE);
            return;
        }

        String sourceSchema = resolveDefaultSchema(sourceConfig);
        String targetSchema = resolveDefaultSchema(targetConfig);

        // 2. Khởi tạo Worker
        MigrationWorker worker = new MigrationWorker(
            this,
            isStructureOnly,
            isDataOnly,
            sourceConfig,
            targetConfig,
            sourceSchema,
            targetSchema,
            1000,
            truncateTarget,
            copyNewOnly,
            limitRows
        );

        // 3. Lắng nghe sự thay đổi của tiến trình (Progress) để cập nhật thanh JProgressBar
        worker.addPropertyChangeListener(evt -> {
            if ("progress".equals(evt.getPropertyName())) {
                int progress = (Integer) evt.getNewValue();
                progressBar.setValue(progress);
            }
        });

        // 4. THỰC THI (Bắt đầu chạy doInBackground ở một thread khác)
        worker.execute();
    }

    private DatabaseConfig buildDatabaseConfig(boolean isSource) {
        String selectedType = isSource
                ? (String) sourceDbTypeCombo.getSelectedItem()
                : (String) targetDbTypeCombo.getSelectedItem();

        DatabaseType dbType = parseDatabaseType(selectedType);
        String host = getRequiredText(isSource ? sourceHostField : targetHostField, "Host");
        String portRaw = getRequiredText(isSource ? sourcePortField : targetPortField, "Port");
        String dbName = getRequiredText(isSource ? sourceDbNameField : targetDbNameField, "DB Name / SID");
        String username = getRequiredText(isSource ? sourceUserField : targetUserField, "Username");
        String password = new String(isSource ? sourcePassField.getPassword() : targetPassField.getPassword());

        int port;
        try {
            port = Integer.parseInt(portRaw.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Port không hợp lệ: " + portRaw);
        }

        return new DatabaseConfig(dbType, host, port, dbName, username, password);
    }

    private static DatabaseType parseDatabaseType(String selectedType) {
        if (selectedType == null) {
            throw new IllegalArgumentException("Database Type không được để trống.");
        }

        if ("Oracle".equalsIgnoreCase(selectedType)) {
            return DatabaseType.ORACLE;
        }
        if ("PostgreSQL".equalsIgnoreCase(selectedType)) {
            return DatabaseType.POSTGRESQL;
        }

        throw new IllegalArgumentException("Database Type chưa được hỗ trợ: " + selectedType);
    }

    private static String resolveDefaultSchema(DatabaseConfig config) {
        if (config.getType() == DatabaseType.ORACLE) {
            return config.getUsername().toUpperCase(Locale.ROOT);
        }
        return "public";
    }

    private static String getRequiredText(JTextField field, String label) {
        String value = field.getText();
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(label + " không được để trống.");
        }
        return value.trim();
    }

    private Integer parseLimitRows() {
        String raw = limitDataField.getText();
        if (raw == null || raw.isBlank()) {
            return null;
        }

        try {
            int parsed = Integer.parseInt(raw.trim());
            if (parsed < 0) {
                throw new IllegalArgumentException("Limit phải >= 0. Dùng 0 để copy toàn bộ.");
            }
            return parsed == 0 ? null : parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Limit không hợp lệ: " + raw);
        }
    }

    /**
     * Hàm tiện ích để ghi log ra màn hình
     */
    public void appendLog(String message) {
        SwingUtilities.invokeLater(() -> {
            logArea.append(message + "\n");
            // Tự động cuộn xuống dòng cuối cùng
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    // --- MAIN METHOD ---
    public static void main(String[] args) {
        // Thiết lập giao diện hệ thống (Native Look and Feel) cho đẹp hơn
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException e) {
            System.err.println("Không thể áp dụng Look and Feel hệ thống: " + e.getMessage());
        }

        SwingUtilities.invokeLater(() -> {
            new MigrationAppUI().setVisible(true);
        });
    }
}