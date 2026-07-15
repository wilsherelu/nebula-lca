type CheckboxProps = {
  checked: boolean;
  disabled?: boolean;
  ariaLabel: string;
  onCheckedChange: (checked: boolean) => void;
};

export function Checkbox({ checked, disabled = false, ariaLabel, onCheckedChange }: CheckboxProps) {
  return (
    <label className={`app-checkbox${disabled ? " is-disabled" : ""}`}>
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        aria-label={ariaLabel}
        onChange={(event) => onCheckedChange(event.target.checked)}
      />
      <span className="app-checkbox-control" aria-hidden="true" />
    </label>
  );
}
